import datetime
import io
import json
import os
import pandas as pd
import logging
import csv
from django.conf import settings
from django.shortcuts import render, redirect
from django.http import HttpResponse, Http404, JsonResponse
from django.contrib import messages
from django.utils.encoding import smart_str
from django.utils.timezone import now
from django.contrib.auth import authenticate, login as auth_login
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from .models import Client
from weasyprint import HTML
from django.template.loader import render_to_string
logger = logging.getLogger(__name__)
import re
import tempfile
import logging
import io
import os
import csv
import datetime
import pandas as pd
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
import datetime as dt
from datetime import datetime
from datetime import datetime as dt
import datetime

# Optional: Import chardet and charset_normalizer for encoding detection
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

try:
    import chardet
except ImportError:
    chardet = None
try:
    import charset_normalizer
except ImportError:
    charset_normalizer = None

def read_file(input_file, function_name="Unknown", user_encoding=None, find_header=False, max_rows_to_search=20):
    """
    Unified file reading function for MYOB to Xero conversion.
    Handles CSV, Excel, and TXT files with encoding and delimiter detection.
    """
    filename = getattr(input_file, 'name', 'unknown_file')
    ext = filename.split('.')[-1].lower()
    
    # Validate file size
    input_file.seek(0, os.SEEK_END)
    file_size = input_file.tell()
    input_file.seek(0)
    if file_size < 10:
        logger.error(f"{filename} is too small or empty")
        raise ValueError("The file is empty or too small to be a valid file.")

    # Read sample for encoding and delimiter detection
    sample_size = min(1024 * 100, file_size)  # 100KB sample
    raw_sample = input_file.read(sample_size)
    input_file.seek(0)

    # Skip null byte check for Excel files (binary by nature)
    if ext not in ['xlsx', 'xls']:
        # Detect encoding
        detected_encoding = 'utf-8'
        if chardet:
            encoding_result = chardet.detect(raw_sample)
            detected_encoding = encoding_result['encoding'] or 'utf-8'
            logger.debug(f"Detected encoding for {filename}: {detected_encoding} (confidence: {encoding_result['confidence']})")
        
        # Check for null bytes (common in UTF-16)
        null_positions = [i for i, b in enumerate(raw_sample[:100]) if b == 0]
        if null_positions:
            logger.debug(f"Null bytes found in {filename} at positions: {null_positions}")
            logger.debug(f"First 100 bytes of {filename}: {raw_sample[:100]}")
            try:
                test_content = raw_sample.decode('utf-16')
                detected_encoding = 'utf-16'
                logger.debug(f"Successfully decoded {filename} as UTF-16")
            except UnicodeDecodeError:
                logger.error(f"{filename} contains null bytes but failed to decode as UTF-16")
                raise ValueError(
                    f"The file contains null bytes at positions {null_positions}, indicating a binary or non-text file. "
                    f"Ensure it is a valid CSV, Excel, or TXT file. Try re-saving as UTF-8 in a text editor. "
                    f"First 100 bytes: {raw_sample[:100]}"
                )
    else:
        detected_encoding = None
        logger.debug(f"Processing {filename} as Excel, skipping text encoding detection")

    encodings = [user_encoding, detected_encoding, 'utf-8-sig', 'utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'utf-16']
    encodings = [e for e in encodings if e]
    encodings = list(dict.fromkeys(encodings))

    if ext not in ['xlsx', 'xls']:
        try:
            raw_content = raw_sample.decode(detected_encoding or 'utf-8', errors='ignore')[:1000]
            logger.debug(f"Raw content of {filename} (first 1000 chars, {detected_encoding or 'N/A'}): %s", raw_content)
        except Exception as e:
            logger.debug(f"Failed to decode raw content of {filename}: %s", str(e))

    df = None
    skiprows = 0
    if ext == 'csv' or ext == 'txt':
        sample = raw_sample.decode(detected_encoding or 'utf-8', errors='replace').replace('\r\n', '\n').replace('\r', '\n')
        delimiters = [',', ';', '\t', '|']
        delimiter = ',' if ext == 'csv' else '\t'
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=delimiters)
            delimiter = dialect.delimiter
            logger.debug(f"Detected delimiter for {function_name}: '{delimiter}'")
        except csv.Error:
            logger.warning(f"Sniffer failed for {function_name}. Testing delimiters: {delimiters}")
            first_line = sample.split('\n')[0]
            delimiter_counts = {d: first_line.count(d) for d in delimiters}
            delimiter = max(delimiter_counts, key=delimiter_counts.get, default=delimiter)
            logger.debug(f"Selected delimiter for {function_name}: '{delimiter}'")

        for encoding in encodings:
            for test_delimiter in delimiters if all(col.startswith('Unnamed:') or col == '{}' for col in (df.columns if df is not None else [])) else [delimiter]:
                try:
                    input_file.seek(0)
                    content = input_file.read().decode(encoding, errors='replace').replace('\r\n', '\n').replace('\r', '\n')
                    content_io = io.StringIO(content)
                    df = pd.read_csv(
                        content_io,
                        sep=test_delimiter,
                        dtype=str,
                        keep_default_na=False,
                        engine='python',
                        skip_blank_lines=True,
                        quoting=csv.QUOTE_MINIMAL,
                        on_bad_lines='skip'
                    )
                    logger.debug(f"Parsed {filename} with encoding='{encoding}', delimiter='{test_delimiter}', columns={df.columns.tolist()}")

                    if not find_header and all(col.startswith('Unnamed:') or col == '{}' for col in df.columns):
                        logger.warning(f"No valid headers detected in {filename}. Searching for header row.")
                        input_file.seek(0)
                        content_io.seek(0)
                        df_raw = pd.read_csv(content_io, header=None, sep=test_delimiter, engine='python')
                        for i in range(min(max_rows_to_search, len(df_raw))):
                            row = df_raw.iloc[i].astype(str)
                            if row.str.contains(r'(Item\s*(Number|Code|No)|Product\s*(ID|Code|Name)|Name|Description|Price|Stock|Customer|Invoice\s*(Number|No)|Account\s*Code|Job\s*(Number|Name))', case=False, regex=True, na=False).any():
                                skiprows = i
                                input_file.seek(0)
                                content_io.seek(0)
                                df = pd.read_csv(
                                    content_io,
                                    sep=test_delimiter,
                                    dtype=str,
                                    keep_default_na=False,
                                    engine='python',
                                    skiprows=skiprows,
                                    skip_blank_lines=True,
                                    quoting=csv.QUOTE_MINIMAL,
                                    on_bad_lines='skip'
                                )
                                logger.debug(f"Found header at row {skiprows} in {filename}, columns={df.columns.tolist()}")
                                break
                        else:
                            logger.warning(f"No valid header found in first {max_rows_to_search} rows of {filename}. Using positional columns.")
                            input_file.seek(0)
                            content_io.seek(0)
                            df = pd.read_csv(
                                content_io,
                                sep=test_delimiter,
                                dtype=str,
                                keep_default_na=False,
                                engine='python',
                                skip_blank_lines=True,
                                quoting=csv.QUOTE_MINIMAL,
                                on_bad_lines='skip',
                                names=['Item Number', 'Item Name'] + [f'Col_{i}' for i in range(2, df.shape[1])]
                            )
                            logger.debug(f"Assigned positional columns: {df.columns.tolist()}")

                    if df.empty or df.shape[1] <= 1:
                        logger.warning(f"CSV/TXT for {function_name} has no valid columns with encoding={encoding}, delimiter={test_delimiter}")
                        df = None
                        continue
                    break
                except Exception as e:
                    logger.debug(f"Failed parsing {filename} with encoding='{encoding}', delimiter='{test_delimiter}': {str(e)}")
                    df = None
                    continue
            if df is not None:
                break

        if df is None or df.empty:
            input_file.seek(0)
            lines = input_file.read().decode(detected_encoding or 'utf-8', errors='ignore').splitlines()
            logger.debug(f"Entire file content (up to 20 lines):\n%s", '\n'.join(lines[:20]))
            raise ValueError(
                f"Invalid {ext.upper()} format for {function_name}. Ensure:\n"
                f"1. Valid delimiters (commas, semicolons, tabs, or pipes; tried: {', '.join(delimiters)}).\n"
                f"2. Encoding is UTF-8 or compatible (tried: {', '.join(encodings)}).\n"
                f"3. A header row with relevant columns (first 20 lines logged above)."
            )

    elif ext in ['xlsx', 'xls']:
        try:
            input_file.seek(0)
            df = pd.read_excel(input_file, engine='openpyxl' if ext == 'xlsx' else 'xlrd')
            logger.debug(f"Parsed {filename} as Excel, columns={df.columns.tolist()}")

            if not find_header and all(col.startswith('Unnamed:') or col == '{}' for col in df.columns):
                logger.warning(f"No valid headers detected in {filename}. Searching for header row.")
                input_file.seek(0)
                df_raw = pd.read_excel(input_file, header=None, engine='openpyxl' if ext == 'xlsx' else 'xlrd')
                for i in range(min(max_rows_to_search, len(df_raw))):
                    row = df_raw.iloc[i].astype(str)
                    if row.str.contains(r'(Item\s*(Number|Code|No)|Product\s*(ID|Code|Name)|Name|Description|Price|Stock|Customer|Invoice\s*(Number|No)|Account\s*Code|Job\s*(Number|Name))', case=False, regex=True, na=False).any():
                        skiprows = i
                        input_file.seek(0)
                        df = pd.read_excel(
                            input_file,
                            header=skiprows,
                            engine='openpyxl' if ext == 'xlsx' else 'xlrd'
                        )
                        logger.debug(f"Found header at row {skiprows} in {filename}, columns={df.columns.tolist()}")
                        break
                else:
                    logger.warning(f"No valid header found in first {max_rows_to_search} rows of {filename}. Using positional columns.")
                    input_file.seek(0)
                    df = pd.read_excel(
                        input_file,
                        engine='openpyxl' if ext == 'xlsx' else 'xlrd',
                        names=['Item Number', 'Item Name'] + [f'Col_{i}' for i in range(2, df.shape[1])]
                    )
                    logger.debug(f"Assigned positional columns: {df.columns.tolist()}")

        except Exception as e:
            logger.error(f"Failed to parse {filename} as Excel: {str(e)}")
            raise ValueError(f"Invalid Excel file: {str(e)}")

    else:
        logger.error(f"Unsupported file extension: {ext}")
        raise ValueError(f"File must be CSV, Excel, or TXT. Got: {ext}")

    if df is None or df.empty:
        logger.error(f"{filename} contains no data")
        raise ValueError("The uploaded file is empty or could not be parsed.")

    if find_header:
        def find_header_row(df_raw):
            for i, row in df_raw.iterrows():
                if row.astype(str).str.contains("ID No", case=False, na=False).any():
                    logger.debug(f"Found header at row {i} in {filename}")
                    return i
            logger.error(f"Header row containing 'ID No' not found in {filename}")
            input_file.seek(0)
            lines = input_file.read().decode(detected_encoding or 'utf-8', errors='ignore').splitlines()
            logger.debug(f"First 20 lines of {filename}:\n%s", '\n'.join(lines[:20]))
            raise ValueError(
                f"Header row containing 'ID No' not found. "
                f"Ensure the file has a column named 'ID No' or similar. First 20 lines logged above."
            )

        input_file.seek(0)
        if ext == 'csv':
            df_raw = pd.read_csv(input_file, header=None, encoding=detected_encoding, engine='python')
        else:
            df_raw = pd.read_excel(input_file, header=None, engine='openpyxl' if ext == 'xlsx' else 'xlrd')
        header_row = find_header_row(df_raw)
        input_file.seek(0)
        if ext == 'csv':
            df = pd.read_csv(input_file, header=header_row, encoding=detected_encoding, dtype=str, engine='python')
        else:
            df = pd.read_excel(input_file, header=header_row, engine='openpyxl' if ext == 'xlsx' else 'xlrd')
        logger.debug(f"Parsed with header at row {header_row}, columns={df.columns.tolist()}")

    df.columns = df.columns.astype(str).str.strip()
    logger.debug(f"Cleaned columns for {filename}: {df.columns.tolist()}")
    logger.debug(f"Column data types:\n{df.dtypes.to_string()}")
    logger.debug(f"First 10 rows:\n{df.head(10).to_string()}")

    return df
    
# Homepage view
def index(request):
    return render(request, 'index.html')

# Login view
def login_page(request):
    if request.method == "POST":
        username = request.POST.get('username')
        password = request.POST.get('password')
        entity_name = request.POST.get('entity_name')
        
        user = authenticate(request, username=username, password=password)
        if user is not None:
            auth_login(request, user)
            if entity_name:
                request.session['entity_name'] = entity_name
            return redirect('main')
        else:
            messages.error(request, "Invalid credentials")
    return render(request, 'login.html')

# Set entity name
def set_entity(request):
    if request.method == 'POST':
        entity_name = request.POST.get('entity_name')
        if entity_name:
            request.session['entity_name'] = entity_name
            return redirect('myob_to_xero')
        else:
            messages.error(request, "Please enter an entity name.")
    return render(request, 'capture_entity_name.html')

# Save entity name via AJAX
# Placeholder for other views
@csrf_exempt
def save_entity_name(request):
    if request.method == 'POST':
        try:
            import json
            data = json.loads(request.body)
            entity_name = data.get('entity_name', '').strip()
            if not entity_name:
                return JsonResponse({'status': 'error', 'message': 'Entity name is required.'}, status=400)
            request.session['entity_name'] = entity_name
            return JsonResponse({'status': 'success', 'message': 'Entity name saved.'})
        except Exception as e:
            logger.exception(f"Error saving entity name: {str(e)}")
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
    return JsonResponse({'status': 'error', 'message': 'Invalid request method.'}, status=400)

# Signup view
def signup(request):
    return render(request, 'signup.html')

# Main view
def main(request):
    entity_name = request.session.get('entity_name', 'Default Company Name')
    return render(request, 'main.html', {'entity_name': entity_name})

# MYOB to Xero form
def myob_to_xero(request):
    entity_name = request.session.get('entity_name', 'MMC')
    return render(request, 'myob-xero.html', {'entity_name': entity_name})

# Dashboard
@login_required
def dashboard(request):
    clients = Client.objects.filter(user=request.user)
    return render(request, 'dashboard.html', {'clients': clients})

#COA Converion
def convert_coa(request):
    if request.method == "POST":
        try:
            input_file = request.FILES.get("coa_file")
            if not input_file:
                logger.error("No file uploaded for COA conversion.")
                messages.error(request, "No file uploaded.")
                return redirect('myob_to_xero')

            entity_name = request.session.get('entity_name')
            if not entity_name:
                logger.error("Entity name not set for COA conversion.")
                messages.error(request, "Entity name not set. Please set it first.")
                return redirect('set_entity')

            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y")
            filename = f"{safe_entity_name}_COA_{date_str}.csv"

            logger.debug(f"Processing COA file: {input_file.name}")
            file_type = input_file.name.split('.')[-1].lower()
            if file_type == 'csv':
                encodings = ['utf-8', 'latin1', 'iso-8859-1']
                df = None
                for encoding in encodings:
                    try:
                        input_file.seek(0)  # Reset file pointer
                        df = pd.read_csv(input_file, encoding=encoding)
                        logger.debug(f"CSV read successfully with encoding: {encoding}")
                        break
                    except UnicodeDecodeError:
                        logger.debug(f"Failed to read CSV with encoding: {encoding}")
                        continue
                if df is None:
                    raise ValueError("Unable to read CSV file with supported encodings.")
            elif file_type in ['xlsx', 'xls']:
                df = pd.read_excel(input_file, engine='openpyxl')
            else:
                logger.error(f"Unsupported file format: {file_type}")
                messages.error(request, "Unsupported file format. Please upload CSV or Excel.")
                return redirect('myob_to_xero')

            # Remove empty rows
            df = df.dropna(how='all')

            df_columns = list(df.columns)

            # Define the mandatory columns for MYOB (and their corresponding Xero columns)
            mandatory_columns = {
                "Account Number": "*Code",
                "Account Name": "*Name",
                "Account Type": "*Type",
                "Tax Code": "*Tax Code",
                "Description": "Description"
            }
            
            # Alert for missing mandatory columns
            missing_columns = []
            for myob_col, xero_col in mandatory_columns.items():
                if myob_col not in df_columns:
                    missing_columns.append(myob_col)
            
            if missing_columns:
                print(f"Alert: The following MYOB columns are missing to convert to Xero format:")
                for col in missing_columns:
                    print(f"- {col}")
            
            # Columns Mapping
            column_mapping = {
                "Account Number": "*Code",
                "Account Name": "*Name",
                "Account Type": "*Type",
                "Tax Code": "*Tax Code",
                "Description": "Description",
                "Dashboard": "Dashboard",
                "Expense Claims": "Expense Claims",
                "Enable Payments": "Enable Payments"
            }
            
            json_columns = list(column_mapping.keys())
            
            common_columns = [cols for cols in df_columns if cols in json_columns]
            filtered_mapping = {cols: column_mapping[cols] for cols in common_columns}
            
            df = df.rename(columns=filtered_mapping)
            
            # Tax Code Mapping (Embedded directly)
            tax_code_mapping = {
                "CAP": "GST on Capital",
                "FRE_Expense": "GST Free Expenses",
                "FRE_Income": "GST Free Income",
                "GST_Expense": "GST on Expenses",
                "GST_Income": "GST on Income",
                "IMP": "GST on Imports",
                "INP": "Input Taxed",
                "N-T": "BAS Excluded",
                "ITS": "BAS Excluded",
                "EXP": "BAS Excluded",
                "": "BAS Excluded"
            }
            
            def map_tax_code(row):
                tax_code = row["*Tax Code"]
                account_type = row["*Type"]
            
                # Handle FRE tax codes
                if tax_code == "FRE":
                    if account_type in ["Income", "Other Income"]:
                        return tax_code_mapping.get("FRE_Income", "GST Free Income")
                    elif account_type in ["Cost of Sales", "Expense", "Other Expense"]:
                        return tax_code_mapping.get("FRE_Expense", "GST Free Expenses")
                    else:
                        return "BAS Excluded"
            
                # Handle GST tax codes
                elif tax_code == "GST":
                    if account_type in ["Income", "Other Income"]:
                        return tax_code_mapping.get("GST_Income", "GST on Income")
                    elif account_type in ["Cost of Sales", "Expense", "Other Expense"]:
                        return tax_code_mapping.get("GST_Expense", "GST on Expenses")
                    else:
                        return "BAS Excluded"
            
                # Default case
                else:
                    return tax_code_mapping.get(tax_code, tax_code)
            
            df["*Tax Code"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
            
            # Account Types Mapping (Embedded directly)
            type_mapping = {
                "Asset": "Current Asset",
                "Other Asset": "Current Asset",
                "Accounts Payable": "Accounts Payable",
                "Accounts Receivable": "Accounts Receivable",
                "Bank": "Bank",
                "Cost of Sales": "Direct Costs",
                "Credit Card": "Bank",
                "Equity": "Equity",
                "Expense": "Expense",
                "Fixed Asset": "Fixed Asset",
                "Income": "Revenue",
                "Liability": "Liability",
                "Long Term Liability": "Liability",
                "Other Current Asset": "Current Asset",
                "Other Current Liability": "Current Liability",
                "Other Expense": "Expense",
                "Other Income": "Other Income",
                "Other Liability": "Current Liability"
            }
            
            df["*Type"] = df["*Type"].map(type_mapping)
            
            # Add missing columns if they don't exist
            if "Dashboard" not in df.columns:
                df.insert(6, "Dashboard", df["*Type"].apply(lambda x: "Yes" if x in ["Bank", "Credit Card"] else "No"))
            
            df.insert(7, "Expense Claims", "No")
            df.insert(8, "Enable Payments", "No")

            # Define the required column order
            order_of_columns = [
                "*Code", "*Name", "*Type", "*Tax Code", "Description", "Dashboard", 
                "Expense Claims", "Enable Payments"
            ]

            # Reindex the DataFrame to match the order_of_columns, filling missing columns with empty strings
            df = df.reindex(columns=order_of_columns)

            # Replace NaN values with empty string for true empty cells
            df = df.fillna('')
            
            output = io.BytesIO()
            df.to_csv(output, index=False, encoding='utf-8')
            csv_data = output.getvalue()

            logger.info(f"COA conversion successful: {filename}")
            response = HttpResponse(csv_data, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response

        except ValueError as ve:
            logger.exception(f"Encoding error in COA conversion: {str(ve)}")
            messages.error(request, str(ve))
            return redirect('myob_to_xero')
        except Exception as e:
            logger.exception(f"Error in COA conversion: {str(e)}")
            messages.error(request, f"Error: {str(e)}")
            return redirect('myob_to_xero')

    return render(request, 'convert_coa.html')

# Vendor Conversion
@csrf_exempt
def convert_vendor(request):
    if request.method == 'POST':
        try:
            input_file = request.FILES.get("vendor_file")
            if not input_file:
                logger.error("No file uploaded for Vendor conversion.")
                return JsonResponse({'error': "No file uploaded. Please select a valid CSV file."}, status=400)

            entity_name = request.session.get('entity_name', 'Default')
            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y")
            filename = f"{safe_entity_name}_Vendor_{date_str}.csv"

            logger.debug(f"Processing Vendor file: {input_file.name}, Size: {input_file.size} bytes")
            if not input_file.name.endswith('.csv'):
                logger.error("Unsupported file format: Only CSV is supported.")
                return JsonResponse({'error': "Unsupported file format. Please upload a CSV file."}, status=400)

            # Read file as raw text to preprocess and diagnose
            input_file.seek(0)
            raw_content = input_file.read().decode('utf-8', errors='ignore').strip()
            # Remove BOM if present
            if raw_content.startswith('\ufeff'):
                raw_content = raw_content[1:]
            # Normalize line endings
            raw_content = raw_content.replace('\r\n', '\n').replace('\r', '\n')
            logger.debug(f"First 200 chars of file:\n{raw_content[:200]}")

            # Detect delimiter
            delimiter = None
            try:
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(raw_content[:1024]).delimiter
                logger.debug(f"Detected delimiter: '{delimiter}'")
            except Exception as e:
                logger.debug(f"Delimiter detection failed: {str(e)}. Trying fallback delimiters.")

            # Try reading CSV with multiple encodings and delimiters
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'utf-16']
            delimiters = [delimiter] if delimiter else [',', ';', '\t', '|', ' ']
            df = None
            for encoding in encodings:
                for delim in delimiters:
                    try:
                        # Convert raw content to bytes for StringIO
                        content_bytes = raw_content.encode(encoding, errors='ignore')
                        content_io = io.StringIO(content_bytes.decode(encoding, errors='ignore'))
                        df = pd.read_csv(content_io, sep=delim, engine='python')
                        logger.debug(f"CSV read successfully with encoding: {encoding}, delimiter: '{delim}'")
                        break
                    except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as e:
                        logger.debug(f"Failed with encoding: {encoding}, delimiter: '{delim}', error: {str(e)}")
                        continue
                if df is not None:
                    break

            if df is None:
                raise ValueError(
                    "Unable to read CSV file with supported encodings or delimiters. "
                    "Please ensure the file is a valid CSV with UTF-8, Latin1, ISO-8859-1, or UTF-16 encoding, "
                    "uses commas (,), semicolons (;), tabs, or pipes (|) as separators, and has valid headers (e.g., 'Co./Last Name'). "
                    "Try opening the file in Notepad++ or VS Code, saving as UTF-8 with comma separators, and ensuring consistent row lengths."
                )

            # Remove empty rows
            df = df.dropna(how='all')

            logger.debug(f"Input columns: {df.columns.tolist()}")
            df.columns = df.columns.str.strip()

            # Remove duplicate columns early
            df = df.loc[:, ~df.columns.duplicated(keep='first')]

            # Column mapping
            column_mapping = {
                "Card ID": "AccountNumber",
                "Addr 1 - Email": "EmailAddress",
                "First Name": "FirstName",
                "Addr 1 - Line 1": "POAddressLine1",
                "Addr 1 - Line 2": "POAddressLine2",
                "Addr 1 - Line 3": "POAddressLine3",
                "Addr 1 - Line 4": "POAddressLine4",
                "Addr 1 - City": "POCity",
                "Addr 1 - State": "PORegion",
                "Addr 1 - Postcode": "POZipCode",
                "Addr 1 - Country": "POCountry",
                "Addr 2 - Line 1": "SAAddressLine1",
                "Addr 2 - Line 2": "SAAddressLine2",
                "Addr 2 - Line 3": "SAAddressLine3",
                "Addr 2 - Line 4": "SAAddressLine4",
                "Addr 2 - City": "SACity",
                "Addr 2 - State": "SARegion",
                "Addr 2 - Postcode": "SAZipCode",
                "Addr 2 - Country": "SACountry",
                "Addr 1 - Phone No. 1": "PhoneNumber",
                "Addr 1 - Fax No.": "FaxNumber",
                "Account Name": "BankAccountName",
                "Account Number": "BankAccountNumber",
                "Statement Text": "BankAccountParticulars",
                "A.B.N.": "TaxNumber",
                " - Balance Due Days": "DueDateBillDay",
                "Terms - Payment is Due": "DueDateBillTerm",
                "Account": "PurchasesAccount",
                "Addr 1 - Contact": "POAttentionTo",
                "Addr 2 - Contact": "SAAttentionTo",
                "Mobile": "MobileNumber",
                "DDI": "DDINumber",
                "Skype": "SkypeName",
                "Tax Type": "TaxNumberType"
            }

            required_columns = ["Co./Last Name"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing Vendor columns: {missing_columns}")
                return JsonResponse({
                    'error': f'Missing required columns: {", ".join(missing_columns)}. Please ensure the CSV contains a "Co./Last Name" column.'
                }, status=400)

            # Set LastName only for individuals
            df["LastName"] = ""
            if "First Name" in df.columns and "Co./Last Name" in df.columns:
                df.loc[df["First Name"].notna(), "LastName"] = df["Co./Last Name"].fillna("")

            # Merge BSB and Account Number
            if "BSB" in df.columns and "Account Number" in df.columns:
                df["BankAccountNumber"] = (df["BSB"].fillna("") + df["Account Number"].fillna("")).str.strip()
                logger.debug("Successfully merged 'BSB' and 'Account Number' into 'BankAccountNumber'.")
            else:
                logger.debug("BSB or Account Number missing, skipping merge.")

            # Create *ContactName
            df["*ContactName"] = (df["First Name"].fillna("") + " " + df["Co./Last Name"].fillna("")).str.strip()

            # Apply column mapping
            df = df.rename(columns=column_mapping)
            
            # Clean AccountNumber
            if "AccountNumber" in df.columns:
                df["AccountNumber"] = df["AccountNumber"].astype(str).str.replace(r'\*', '', regex=True)
                df["AccountNumber"] = df["AccountNumber"].replace("None", "").str.strip()

            # Final column order
            final_column_order = [
                "*ContactName", "AccountNumber", "EmailAddress", "FirstName", "LastName",
                "POAttentionTo", "POAddressLine1", "POAddressLine2", "POAddressLine3", 
                "POAddressLine4", "POCity", "PORegion", "POZipCode", "POCountry", 
                "SAAttentionTo", "SAAddressLine1", "SAAddressLine2", "SAAddressLine3", 
                "SAAddressLine4", "SACity", "SARegion", "SAZipCode", "SACountry", 
                "PhoneNumber", "FaxNumber", "MobileNumber", "DDINumber", "SkypeName", 
                "BankAccountName", "BankAccountNumber", "BankAccountParticulars", 
                "TaxNumberType", "TaxNumber", "DueDateBillDay", "DueDateBillTerm", "PurchasesAccount"
            ]

            # Ensure all columns exist
            for col in final_column_order:
                if col not in df.columns:
                    df[col] = ""
            df = df[final_column_order]

            # Log output sample
            logger.debug(f"Output columns: {df.columns.tolist()}")
            logger.debug(f"Output sample:\n{df.head().to_string()}")

            output = io.BytesIO()
            df.to_csv(output, index=False, encoding='utf-8')
            csv_data = output.getvalue()

            logger.info(f"Vendor conversion successful: {filename}")
            response = HttpResponse(csv_data, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response

        except ValueError as ve:
            logger.exception(f"Encoding or parsing error in Vendor conversion: {str(ve)}")
            return JsonResponse({
                'error': (
                    f"{str(ve)} Try the following: "
                    "1. Open the file in Notepad++ or VS Code and check for unusual characters or inconsistent row lengths. "
                    "2. Save it as CSV with UTF-8 encoding and comma (,) separators. "
                    "3. Ensure it has valid headers (e.g., 'Co./Last Name') and consistent columns."
                )
            }, status=400)
        except Exception as e:
            logger.exception(f"Error in Vendor conversion: {str(e)}")
            return JsonResponse({
                'error': (
                    f"Vendor conversion failed: {str(e)}. Please verify the file is a valid CSV with the required columns "
                    "(e.g., 'Co./Last Name'), uses standard encoding (UTF-8), and has consistent row lengths."
                )
            }, status=500)

    return JsonResponse({'error': 'Invalid request method. Please use POST.'}, status=400)

# Manual Journal Conversion
@csrf_exempt
def convert_manual_journal(request):
    logger.debug("convert_manual_journal view called")
    
    if request.method != 'POST':
        logger.error("Invalid method: Only POST method is allowed.")
        return JsonResponse({'error': "Only POST method is allowed."}, status=405)

    try:
        input_file = request.FILES.get("manual_journal_file")
        if not input_file:
            logger.error("No file uploaded for Manual Journal conversion.")
            return JsonResponse({'error': 'No file uploaded.'}, status=400)

        entity_name = request.session.get('entity_name', 'Default')
        safe_entity_name = entity_name.replace(" ", "_")
        date_str = dt.now().strftime("%d-%m-%Y")
        filename = f"{safe_entity_name}_ManualJournal_{date_str}.csv"

        logger.debug(f"Processing Manual Journal file: {input_file.name}, Size: {input_file.size} bytes")
        file_type = input_file.name.split('.')[-1].lower()
        if file_type not in ['csv', 'xlsx', 'xls']:
            logger.error(f"Unsupported file format: {file_type}")
            return JsonResponse({'error': 'Unsupported file format. Please upload CSV or Excel.'}, status=400)

        # Read file with header detection for 'ID No'
        user_encoding = request.POST.get('file_encoding') if file_type == 'csv' else None
        df = read_file(input_file, function_name="Manual Journal", user_encoding=user_encoding, find_header=True)

        # Log first few rows for debugging
        logger.debug(f"First 10 rows of {input_file.name}:\n{df.head(10).to_string()}")

        # Placeholder: Add your Manual Journal processing logic here
        required_columns = ["ID No", "Date", "Account Code"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing Manual Journal columns: {missing_columns}. Available columns: {df.columns.tolist()}")
            return JsonResponse({
                'error': f"Missing required columns: {', '.join(missing_columns)}. Available columns: {', '.join(df.columns)}"
            }, status=400)

        # Example output (replace with actual logic)
        output_df = df[required_columns]
        output = io.BytesIO()
        output_df.to_csv(output, index=False, encoding="utf-8")
        csv_data = output.getvalue()

        logger.info(f"Manual Journal conversion successful: {filename}")
        response = HttpResponse(csv_data, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['X-Content-Type-Options'] = 'nosniff'
        return response

    except ValueError as ve:
        logger.exception(f"Encoding error in Manual Journal conversion: {str(ve)}")
        return JsonResponse({'error': str(ve)}, status=400)
    except Exception as e:
        logger.exception(f"Error in Manual Journal conversion: {str(e)}")
        return JsonResponse({'error': f"Manual Journal conversion failed: {str(e)}"}, status=500)

@csrf_exempt
def convert_customer(request):
    if request.method == 'POST':
        try:
            input_file = request.FILES.get("customer_file")
            if not input_file:
                logger.error("No file uploaded")
                return JsonResponse({'error': "No file uploaded. Select a CSV, Excel, or TXT file."}, status=400)

            filename = input_file.name
            logger.debug(f"Processing file: {filename}")
            ext = filename.split('.')[-1].lower()
            if ext not in ['csv', 'xls', 'xlsx', 'txt']:
                logger.error(f"Invalid file extension: {ext}")
                return JsonResponse({'error': "File must be a CSV, Excel, or TXT."}, status=400)

            # Read the file
            df = read_file(input_file, filename)

            # Log DataFrame info
            logger.debug("Customer file first 3 rows:\n%s", df.head(3).to_string())

            # Handle headerless CSVs
            actual_columns = [str(col).strip().lower() for col in df.columns]
            if all(col.startswith('unnamed:') or col in ['nan', '{}'] for col in actual_columns):
                if len(df.columns) >= 2:
                    df.columns = ["First Name", "Co./Last Name"] + [f"Extra_{i}" for i in range(len(df.columns) - 2)]
                    logger.debug(f"Assigned default columns for {filename}: %s", list(df.columns))
                else:
                    logger.error(f"{filename} lacks header and has too few columns: %s", len(df.columns))
                    return JsonResponse({
                        'error': f"The file lacks a header row and has fewer than 2 columns. Expected at least: 'First Name', 'Co./Last Name'."
                    }, status=400)

            # Required columns
            required_columns = ["First Name", "Co./Last Name"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns in customer file: {missing_columns}")
                return JsonResponse({
                    'error': f"Missing required columns: {', '.join(missing_columns)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Optional columns
            optional_columns = ["Card ID", "BSB", "Account Number"]
            missing_optional = [col for col in optional_columns if col not in df.columns]
            if missing_optional:
                logger.debug(f"Optional columns missing: {missing_optional}. Adding as empty.")
                for col in missing_optional:
                    df[col] = pd.NA
                    logger.debug(f"Added empty '{col}' column")

            # Column mapping
            column_mapping = {
                "Co./Last Name": "*ContactName",
                "Card ID": "AccountNumber",
                "Addr 1 - Line 1": "POAddressLine1",
                "Addr 1 - Line 2": "POAddressLine2",
                "Addr 1 - Line 3": "POAddressLine3",
                "Addr 1 - Line 4": "POAddressLine4",
                "Addr 1 - City": "POCity",
                "Addr 1 - State": "PORegion",
                "Addr 1 - Postcode": "POPostalCode",
                "Addr 1 - Country": "POCountry",
                "Addr 1 - Phone No. 1": "PhoneNumber",
                "Addr 1 - Phone No. 2": "MobileNumber",
                "Addr 1 - Fax No.": "FaxNumber",
                "Addr 1 - Email": "EmailAddress",
                "Addr 1 - WWW": "Website",
                "Addr 1 - Salutation": "SAAttentionTo",
                "Addr 2 - Line 1": "SAAddressLine1",
                "Addr 2 - Line 2": "SAAddressLine2",
                "Addr 2 - Line 3": "SAAddressLine3",
                "Addr 2 - Line 4": "SAAddressLine4",
                "Addr 2 - City": "SACity",
                "Addr 2 - State": "SARegion",
                "Addr 2 - Postcode": "SAPostalCode",
                "Addr 2 - Country": "SACountry",
                "- % Discount": "Discount",
                "Tax ID No.": "TaxNumber",
                "Account Number": "BankAccountNumber",
                "Account Name": "BankAccountName",
                "A.B.N.": "TaxNumber",
                "Account": "SalesAccount",
                "- Balance Due Days": "DueDateSalesDay",
                "Terms - Payment is Due": "DueDateSalesTerm"
            }

            # Combine First Name and Co./Last Name into ContactName
            try:
                df["*ContactName"] = df["First Name"].fillna('') + " " + df["Co./Last Name"].fillna('')
                df["*ContactName"] = df["*ContactName"].str.strip()
                logger.debug("Created *ContactName column")
            except Exception as e:
                logger.error(f"Error creating *ContactName: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to combine 'First Name' and 'Co./Last Name': {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Combine BSB and Account Number
            try:
                df["BankAccountNumber"] = df.apply(
                    lambda row: f"{row['BSB'] if pd.notna(row['BSB']) else 'Unknown'}-{row['Account Number'] if pd.notna(row['Account Number']) else 'Unknown'}",
                    axis=1
                )
                logger.debug("Created BankAccountNumber column")
            except Exception as e:
                logger.error(f"Error creating BankAccountNumber: {str(e)}")
                df["BankAccountNumber"] = 'Unknown'
                logger.debug("Set BankAccountNumber to 'Unknown' due to error")

            # Remove First Name and Co./Last Name columns
            try:
                df = df.drop(columns=["First Name", "Co./Last Name"], errors='ignore')
                logger.debug("Dropped First Name and Co./Last Name columns")
            except Exception as e:
                logger.error(f"Error dropping columns: {str(e)}")

            # Apply column mapping
            try:
                df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
                logger.debug(f"Applied column mapping. New columns: {list(df.columns)}")
            except Exception as e:
                logger.error(f"Error applying column mapping: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to map columns: {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Keep only valid columns
            valid_columns = list(column_mapping.values()) + ["*ContactName", "BankAccountNumber"]
            try:
                df = df[[col for col in df.columns if col in valid_columns]]
                logger.debug(f"Filtered to valid columns: {list(df.columns)}")
            except Exception as e:
                logger.error(f"Error filtering columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to filter columns: {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Clean AccountNumber
            if 'AccountNumber' in df.columns:
                try:
                    df['AccountNumber'] = df['AccountNumber'].fillna(pd.NA).replace(["*None", ""], pd.NA)
                    logger.debug(f"Cleaned AccountNumber column: {df['AccountNumber'].head().to_string()}")
                except Exception as e:
                    logger.error(f"Error cleaning AccountNumber: {str(e)}")
                    df['AccountNumber'] = pd.NA
                    logger.debug("Set AccountNumber to NA due to error")
            else:
                logger.debug("AccountNumber column not found. Adding as empty.")
                df['AccountNumber'] = pd.NA

            # Rearrange columns with ContactName first
            try:
                cols = ["*ContactName"] + [col for col in df.columns if col != "*ContactName"]
                df = df[cols]
                logger.debug(f"Rearranged columns: {list(df.columns)}")
            except Exception as e:
                logger.error(f"Error rearranging columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to rearrange columns: {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Generate output CSV
            entity_name = request.session.get('entity_name', 'Default').replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M")
            filename = f"{entity_name}_Customer_{date_str}.csv"

            output = io.StringIO()
            df.to_csv(output, index=False, encoding='utf-8-sig')
            csv_data = output.getvalue()

            response = HttpResponse(csv_data, content_type='text/csv; charset=utf-8')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            logger.info(f"Successfully generated Customer CSV: {filename}")
            return response

        except ValueError as ve:
            logger.error(f"ValueError in convert_customer: {str(ve)}")
            return JsonResponse({
                'error': str(ve)
            }, status=400)
        except Exception as e:
            logger.exception(f"Customer conversion error: {str(e)}")
            return JsonResponse({
                'error': f"Customer conversion failed: {str(e)}. Please verify the file is a valid CSV with the required columns ('First Name', 'Co./Last Name') and optional 'Card ID' for AccountNumber. Found columns: {', '.join(df.columns) if 'df' in locals() else 'unknown'}."
            }, status=500)

    logger.error("Invalid request method: %s", request.method)
    return JsonResponse({'error': 'Use POST.'}, status=400)

# Job Conversion
@csrf_exempt
def convert_job(request):
    if request.method == 'POST':
        try:
            job_file = request.FILES.get("job_file")
            if not job_file:
                logger.error("No file uploaded for Job conversion.")
                return JsonResponse({'error': 'Please upload Job CSV file.'}, status=400)

            entity_name = request.session.get('entity_name', 'Default')
            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y")
            filename = f"{safe_entity_name}_Job_{date_str}.csv"

            logger.debug(f"Processing Job file: {job_file.name}")
            if not job_file.name.endswith('.csv'):
                logger.error("Unsupported file format: Job file must be CSV.")
                return JsonResponse({'error': 'Job file must be CSV.'}, status=400)

            user_encoding = request.POST.get('file_encoding')
            df = read_csv_with_encoding(job_file, function_name="Job", user_encoding=user_encoding)

            # Remove empty rows
            df = df.dropna(how='all')

            df.columns = df.columns.str.strip()

            required_columns = {"Job Number", "Job Name"}
            if not required_columns.issubset(df.columns):
                missing = required_columns - set(df.columns)
                logger.error(f"Missing Job columns: {missing}")
                return JsonResponse({'error': f'Missing required columns: {", ".join(missing)}'}, status=400)

            # Transform
            df["Job"] = df["Job Number"].astype(str) + "-" + df["Job Name"].astype(str)
            df.drop(columns=["Job Name"], inplace=True)
            df = df[["Job"]]

            output = io.BytesIO()
            df.to_csv(output, index=False, encoding="utf-8")
            csv_data = output.getvalue()

            logger.info(f"Job conversion successful: {filename}")
            response = HttpResponse(csv_data, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response

        except ValueError as ve:
            logger.exception(f"Encoding error in Job conversion: {str(ve)}")
            return JsonResponse({'error': str(ve)}, status=400)
        except Exception as e:
            logger.exception(f"Error in Job conversion: {str(e)}")
            return JsonResponse({'error': f'Job conversion failed: {str(e)}'}, status=500)

    return JsonResponse({'error': 'Invalid request method.'}, status=400)

# Item Invoice Conversion
def convert_item_invoice(request):
    if request.method != 'POST':
        logger.error("Invalid request method: %s", request.method)
        return HttpResponse('Method not allowed', status=405)

    if 'item_invoice_file' not in request.FILES:
        logger.error("No file uploaded in request")
        return HttpResponse('No file uploaded', status=400)

    file_obj = request.FILES['item_invoice_file']
    filename = file_obj.name
    logger.debug("Processing file: %s", filename)

    try:
        ext = filename.split('.')[-1].lower()
        if ext not in ["csv", "xls", "xlsx"]:
            logger.error("Unsupported file extension: %s", ext)
            return HttpResponse(f"Unsupported file extension: {ext}. Please upload a CSV or Excel file.", status=400)

        if ext == "csv":
            try:
                temp_df = pd.read_csv(file_obj, encoding='utf-8', header=None)
            except UnicodeDecodeError:
                logger.debug("UTF-8 encoding failed, trying ISO-8859-1")
                file_obj.seek(0)
                temp_df = pd.read_csv(file_obj, encoding='ISO-8859-1', header=None)
            except pd.errors.EmptyDataError:
                logger.error("Uploaded CSV file is empty")
                return HttpResponse("The uploaded CSV file is empty.", status=400)
        else:
            try:
                temp_df = pd.read_excel(file_obj, header=None)
            except ValueError:
                logger.error("Invalid Excel file format")
                return HttpResponse("The uploaded Excel file is invalid or corrupted.", status=400)

        if temp_df.empty:
            logger.error("File contains no data")
            return HttpResponse("The uploaded file contains no data.", status=400)

        # Flexible column name matching
        temp_df.columns = temp_df.columns.str.strip()
        header_row = temp_df[temp_df.apply(lambda row: row.astype(str).str.contains('Invoice No\.?|Invoice Number|Inv No', case=False, na=False, regex=True)).any(axis=1)]
        if header_row.empty:
            logger.error("Header row with 'Invoice No.' not found. Columns found: %s", temp_df.iloc[0].tolist())
            return HttpResponse(f"Header row with 'Invoice No.' not found. Columns found: {', '.join(temp_df.iloc[0].astype(str).tolist())}", status=400)
        
        header_row_idx = header_row.index[0]
        logger.debug("Header row detected at index: %d", header_row_idx)

        file_obj.seek(0)
        if ext == "csv":
            try:
                df = pd.read_csv(file_obj, skiprows=header_row_idx)
            except pd.errors.EmptyDataError:
                logger.error("No data after skipping header rows")
                return HttpResponse("No valid data found after the header row.", status=400)
        else:
            df = pd.read_excel(file_obj, skiprows=header_row_idx)

        df.columns = df.columns.str.strip()
        # Map possible column name variations
        column_variations = {
            "Invoice No.": ["Invoice No.", "Invoice Number", "Inv No", "Invoice"],
            "Date": ["Date", "Invoice Date", "Created Date"],
            "Customer": ["Customer", "Client", "Customer Name"],
            "Amount": ["Amount", "Total", "Invoice Amount"]
        }

        # Find matching columns
        column_mapping = {}
        for required, variants in column_variations.items():
            for col in df.columns:
                if col in variants or any(v.lower() in col.lower() for v in variants):
                    column_mapping[col] = required
                    break

        missing_columns = [req for req in column_variations if not any(col in column_mapping for col in df.columns if col in column_mapping and column_mapping[col] == req)]
        if missing_columns:
            logger.error("Missing required columns: %s. Found columns: %s", missing_columns, df.columns.tolist())
            return HttpResponse(f"Missing required columns: {', '.join(missing_columns)}. Found columns: {', '.join(df.columns)}", status=400)

        # Rename columns to standard names
        df.rename(columns=column_mapping, inplace=True)
        required_columns = ["Invoice No.", "Date", "Customer", "Amount"]
        df = df[required_columns]

        # Map to output format
        output_mapping = {
            "Invoice No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Customer": "*ContactName",
            "Amount": "*UnitAmount"
        }
        df.rename(columns=output_mapping, inplace=True)

        df['*DueDate'] = df['*InvoiceDate']
        df['Description'] = "Item Invoice"
        df['*Quantity'] = 1
        df["*TaxType"] = "BAS Excluded"
        df['LineAmountType'] = "Exclusive"
        df["*AccountCode"] = "200"

        df = df[df['*InvoiceNumber'].notna()]
        if df.empty:
            logger.error("No rows with valid InvoiceNumber after filtering")
            return HttpResponse("No rows with valid InvoiceNumber found.", status=400)

        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        response = HttpResponse(
            content=output.getvalue(),
            content_type='text/csv'
        )
        response['Content-Disposition'] = 'attachment; filename="XERO_ITEM_INVOICE.csv"'
        logger.info("Successfully generated CSV response")
        return response

    except Exception as e:
        logger.exception("Error processing file: %s", str(e))
        return HttpResponse(f"Error processing file: {str(e)}", status=500)

# Item Master Conversion
@csrf_exempt
def convert_item_master(request):
    if request.method == 'POST':
        try:
            input_file = request.FILES.get("item_master_file")
            if not input_file:
                logger.error("No file uploaded for Item Master conversion.")
                return JsonResponse({'error': 'No file uploaded.'}, status=400)

            entity_name = request.session.get('entity_name', 'Default')
            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y")
            filename = f"{safe_entity_name}_ItemMaster_{date_str}.csv"

            logger.debug(f"Processing Item Master file: {input_file.name}, Size: {input_file.size} bytes")
            file_type = input_file.name.split('.')[-1].lower()
            if file_type not in ['csv', 'xlsx', 'xls']:
                logger.error(f"Unsupported file format: {file_type}")
                return JsonResponse({'error': 'Unsupported file format. Please upload CSV or Excel.'}, status=400)

            # Read file
            user_encoding = request.POST.get('file_encoding') if file_type == 'csv' else None
            df = read_file(input_file, function_name="Item Master", user_encoding=user_encoding, find_header=False)

            # Column mapping
            column_mapping = {
                "Item Number": "*Code",
                "Item Name": "*Name",
                "Description": "*Description",
                "Selling Price": "*SalesUnitPrice",
                "Purchase Price": "*PurchaseUnitPrice",
                "Sales Account": "*SalesAccount",
                "Purchase Account": "*PurchaseAccount",
                "Tax Code Sales": "*SalesTaxType",
                "Tax Code Purchase": "*PurchaseTaxType",
                "Unit of Measure": "*UnitOfMeasure"
            }

            # Synonyms for MYOB column names
            column_synonyms = {
                "Item Number": ["Item Number", "Item Code", "Product ID", "Code", "Item No", "Product Code"],
                "Item Name": ["Item Name", "Name", "Product Name", "Item Description", "Description"]
            }

            # Check for required columns or their synonyms
            required_columns = ["Item Number", "Item Name"]
            actual_cols = df.columns.tolist()
            missing_columns = []
            rename_dict = {}

            for req_col in required_columns:
                found = False
                for syn in column_synonyms[req_col]:
                    if syn in actual_cols:
                        rename_dict[syn] = column_mapping[req_col]
                        logger.debug(f"Found synonym '{syn}' for required column '{req_col}'")
                        found = True
                        break
                if not found:
                    missing_columns.append(req_col)

            if missing_columns:
                logger.error(f"Missing Item Master columns: {missing_columns}. Available columns: {actual_cols}")
                return JsonResponse({
                    'error': f'Missing required columns: {", ".join(missing_columns)}. Available columns: {", ".join(actual_cols)}'
                }, status=400)

            df = df.rename(columns=rename_dict)
            logger.debug(f"Renamed columns: {df.columns.tolist()}")

            # Tax type mapping
            tax_type_mapping = {
                "GST": "GST on Income",
                "FRE": "GST Free Income",
                "N-T": "BAS Excluded",
                "": "BAS Excluded"
            }
            if "*SalesTaxType" in df.columns:
                df["*SalesTaxType"] = df["*SalesTaxType"].map(tax_type_mapping).fillna("BAS Excluded")
            if "*PurchaseTaxType" in df.columns:
                df["*PurchaseTaxType"] = df["*PurchaseTaxType"].map(tax_type_mapping).fillna("BAS Excluded")

            # Final column order
            final_column_order = [
                "*Code", "*Name", "*Description", "*SalesUnitPrice", "*PurchaseUnitPrice",
                "*SalesAccount", "*PurchaseAccount", "*SalesTaxType", "*PurchaseTaxType", "*UnitOfMeasure"
            ]

            for col in final_column_order:
                if col not in df.columns:
                    df[col] = ""
            df = df[final_column_order]

            output = io.BytesIO()
            df.to_csv(output, index=False, encoding="utf-8")
            csv_data = output.getvalue()

            logger.info(f"Item Master conversion successful: {filename}")
            response = HttpResponse(csv_data, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response

        except ValueError as ve:
            logger.exception(f"Encoding error in Item Master conversion: {str(ve)}")
            return JsonResponse({'error': str(ve)}, status=400)
        except Exception as e:
            logger.exception(f"Error in Item Master conversion: {str(e)}")
            return JsonResponse({'error': f'Item Master conversion failed: {str(e)}'}, status=500)

    return JsonResponse({'error': 'Invalid request method.'}, status=400)

@csrf_exempt
def convert_sales_invoice_product(request):
    logger.debug("convert_sales_invoice_product view called")
    
    if request.method != 'POST':
        logger.error("Invalid method: Only POST method is allowed.")
        return JsonResponse({'error': "Only POST method is allowed."}, status=405)

    try:
        logger.debug(f"Received files: {list(request.FILES.keys())}")
        logger.debug(f"Received POST data: {dict(request.POST)}")
        
        # Corrected key for product_invoice_file
        product_invoice_file = request.FILES.get("sales_invoice_product_file")
        coa_file = request.FILES.get("coa_file_product")
        job_file = request.FILES.get("job_file_product")

        logger.debug(f"product_invoice_file: {product_invoice_file.name if product_invoice_file else 'None'}")
        logger.debug(f"coa_file: {coa_file.name if coa_file else 'None'}")
        logger.debug(f"job_file: {job_file.name if job_file else 'None'}")

        # Check for missing files
        missing_files = []
        if not product_invoice_file:
            missing_files.append("Product Invoice")
        if not coa_file:
            missing_files.append("COA")
        if not job_file:
            missing_files.append("Job")
        if missing_files:
            logger.error(f"Missing required files: {', '.join(missing_files)}")
            return JsonResponse({
                'error': f"Missing required files: {', '.join(missing_files)}. "
                        f"Please upload Product Invoice, COA, and Job files with names 'sales_invoice_product_file', 'coa_file_product', and 'job_file_product'. "
                        f"Received files: {list(request.FILES.keys())}. Received POST: {dict(request.POST)}"
            }, status=400)

        # Get entity name and set up filename
        entity_name = request.session.get('entity_name', 'Default')
        safe_entity_name = entity_name.replace(" ", "_")
        date_str = dt.now().strftime("%d-%m-%Y_%H-%M")
        filename = f"{safe_entity_name}_Product_Invoice_{date_str}.csv"
        logger.debug(f"Generated filename: {filename}")

        # Read files
        logger.debug("Reading files")
        user_encoding = request.POST.get('file_encoding')
        invoice_df = read_file(product_invoice_file, function_name="Product Invoice", user_encoding=user_encoding, find_header=False)
        coa_df = read_file(coa_file, function_name="COA", user_encoding=user_encoding, find_header=False)
        job_df = read_file(job_file, function_name="Job", user_encoding=user_encoding, find_header=False)

        # Clean columns by stripping any leading/trailing spaces
        invoice_df.columns = invoice_df.columns.str.strip()
        coa_df.columns = coa_df.columns.str.strip()
        job_df.columns = job_df.columns.str.strip()
        logger.debug(f"Invoice columns: {invoice_df.columns.tolist()}")
        logger.debug(f"COA columns: {coa_df.columns.tolist()}")
        logger.debug(f"Job columns: {job_df.columns.tolist()}")

        # Flexible column name matching
        column_variations = {
            "Product Invoice": {
                "Customer": ["Customer", "Client", "Customer Name"],
                "Invoice Number": ["Invoice Number", "Invoice No", "Inv No"],
                "Date": ["Date", "Invoice Date", "Created Date"],
                "Item Code": ["Item Code", "Product Code", "Item Number"],
                "Quantity": ["Quantity", "Qty", "Units"]
            },
            "COA": {
                "Account Code": ["Account Code", "Account", "Acct Code"],
                "Account Name": ["Account Name", "Acct Name", "Name"]
            },
            "Job": {
                "Job Number": ["Job Number", "Job No", "Job ID"],
                "Job Name": ["Job Name", "Project Name", "Job Description"]
            }
        }

        # Validate columns for each file
        for df, required, name in [
            (invoice_df, column_variations["Product Invoice"], "Product Invoice"),
            (coa_df, column_variations["COA"], "COA"),
            (job_df, column_variations["Job"], "Job")
        ]:
            missing = []
            for req, variants in required.items():
                if not any(col in df.columns or any(v.lower() in col.lower() for v in variants) for col in df.columns):
                    missing.append(req)
            if missing:
                logger.error(f"Missing required columns in {name}: {missing}. Found columns: {df.columns.tolist()}")
                return JsonResponse({
                    'error': f"Missing required columns in {name}: {', '.join(missing)}. Found columns: {', '.join(df.columns)}"
                }, status=400)

        # Rename columns to standard names
        for df, variations in [
            (invoice_df, column_variations["Product Invoice"]),
            (coa_df, column_variations["COA"]),
            (job_df, column_variations["Job"])
        ]:
            mapping = {}
            for req, variants in variations.items():
                for col in df.columns:
                    if col in variants or any(v.lower() in col.lower() for v in variants):
                        mapping[col] = req
                        break
            df.rename(columns=mapping, inplace=True)

        # Merge COA and Job data with the Product Invoice
        logger.debug("Merging dataframes")
        output_df = invoice_df.merge(coa_df, left_on="Item Code", right_on="Account Code", how="left")
        output_df = output_df.merge(job_df, left_on="Invoice Number", right_on="Job Number", how="left")

        # Rename columns as per required output format
        output_df.rename(columns={
            "Customer": "*ContactName",
            "Invoice Number": "InvoiceNumber",
            "Date": "*InvoiceDate",
            "Item Code": "LineItemItemCode",
            "Account Name": "LineItemAccountName",
            "Job Name": "TrackingCategory",
            "Quantity": "*Quantity"
        }, inplace=True)

        # Optional fields
        output_df["*UnitAmount"] = invoice_df.get("Unit Amount", "")
        output_df["TaxAmount"] = invoice_df.get("Tax Amount", "")

        # Final column order
        output_df = output_df[[
            "*ContactName", "InvoiceNumber", "*InvoiceDate", "LineItemItemCode",
            "LineItemAccountName", "TrackingCategory", "*Quantity", "*UnitAmount", "TaxAmount"
        ]]
        logger.debug(f"Output columns: {output_df.columns.tolist()}")

        # Create in-memory CSV buffer
        output_buffer = io.BytesIO()
        output_df.to_csv(output_buffer, index=False, encoding='utf-8')
        output_buffer.seek(0)

        # Prepare response with file content
        response = HttpResponse(
            content=output_buffer.getvalue(),
            content_type='text/csv'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['X-Content-Type-Options'] = 'nosniff'

        logger.info(f"Product Invoice conversion successful, returning file: {filename}")
        return response

    except ValueError as ve:
        logger.exception(f"Encoding error in Product Invoice conversion: {str(ve)}")
        return JsonResponse({'error': str(ve)}, status=400)
    except Exception as e:
        logger.exception(f"Error in Product Invoice conversion: {str(e)}")
        return JsonResponse({'error': f"Product Invoice conversion failed: {str(e)}"}, status=500)

        
# Service Invoice Conversion
@csrf_exempt
def convert_sales_invoice_service(request):
    logger.debug("convert_sales_invoice_service view called")
    logger.debug(f"Request method: {request.method}")
    logger.debug(f"Request headers: {dict(request.headers)}")
    logger.debug(f"Request content-type: {request.content_type}")
    logger.debug(f"Request body size: {request.META.get('CONTENT_LENGTH', 'unknown')} bytes")
    logger.debug(f"Request FILES: {dict(request.FILES)}")
    logger.debug(f"Request POST data: {dict(request.POST)}")
    logger.debug(f"Request FILES keys: {list(request.FILES.keys())}")

    if request.method != 'POST':
        logger.error("Invalid method: Only POST method is allowed.")
        return JsonResponse({'error': "Only POST method is allowed."}, status=405)

    if not request.content_type.startswith('multipart/form-data'):
        logger.error(f"Invalid content-type: Expected 'multipart/form-data', got '{request.content_type}'")
        return JsonResponse({'error': "Request must use multipart/form-data encoding."}, status=400)

    try:
        logger.debug(f"Received files: {list(request.FILES.keys())}")
        required_files = {
            "service_invoice_file": "Service Invoice",
            "coa_file_service": "COA",
            "job_file_service": "Job"
        }

        missing_or_invalid_files = []
        uploaded_files = {}
        
        for file_key, file_label in required_files.items():
            if file_key not in request.FILES:
                missing_or_invalid_files.append(f"{file_label} (missing)")
                logger.error(f"File {file_key} is missing from request.FILES")
                continue
            file_obj = request.FILES[file_key]
            logger.debug(f"Processing file {file_key}: name={file_obj.name}, size={file_obj.size}, content_type={file_obj.content_type}")
            if file_obj.size < 10:
                missing_or_invalid_files.append(f"{file_label} (empty or too small)")
                logger.error(f"File {file_key} is empty or too small (size: {file_obj.size} bytes)")
                continue
            uploaded_files[file_key] = file_obj

        if missing_or_invalid_files:
            error_msg = (
                f"No file uploaded or invalid files: {', '.join(missing_or_invalid_files)}. "
                f"Please upload valid Service Invoice (CSV or Excel), COA (CSV), and Job (CSV) files."
            )
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=400)

        df = read_file(uploaded_files["service_invoice_file"], os.path.basename(uploaded_files["service_invoice_file"].name))
        df_coa = read_file(uploaded_files["coa_file_service"], os.path.basename(uploaded_files["coa_file_service"].name))
        df_jobs = read_file(uploaded_files["job_file_service"], os.path.basename(uploaded_files["job_file_service"].name))

        df = df.dropna(how='all')
        df.columns = df.columns.str.strip()
        df_coa.columns = df_coa.columns.str.strip()
        df_jobs.columns = df_jobs.columns.str.strip()

        column_mapping = {
            "Co./Last Name": "*ContactName",
            "Invoice No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Balance Due Days": "*DueDate",
            "Customer PO": "Reference",
            "Description": "*Description",
            "Account No.": "*AccountCode",
            "Amount": "*UnitAmount",
            "Job": "TrackingOption1",
            "Tax Code": "*TaxType",
            "Tax Amount": "TaxAmount",
            "Currency Code": "Currency",
            "Exchange Rate": "Exchange Rate"
        }
        df.rename(columns={col: column_mapping.get(col, col) for col in df.columns}, inplace=True)

        df["*ContactName"] = df["First Name"].fillna('').astype(str) + " " + df["*ContactName"].fillna('').astype(str)
        df["*DueDate"] = df["*InvoiceDate"]
        df["*Description"] = df["*Description"].fillna(".")

        tax_code_mapping = {
            "CAP": "GST on Capital",
            "FRE_Expense": "GST Free Expenses",
            "FRE_Income": "GST Free Income",
            "GST_Expense": "GST on Expenses",
            "GST_Income": "GST on Income",
            "IMP": "GST on Imports",
            "INP": "Input Taxed",
            "N-T": "BAS Excluded",
            "ITS": "BAS Excluded",
            "EXP": "BAS Excluded",
            "": "BAS Excluded"
        }

        def map_tax_code(row):
            account_code = str(int(float(row["*AccountCode"]))) if pd.notna(row["*AccountCode"]) else ""
            tax_code = row["*TaxType"]
            df_coa["Account Number"] = df_coa["Account Number"].astype(str)
            coa_row = df_coa[df_coa["Account Number"] == account_code]

            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            account_type = coa_row.iloc[0]["Account Type"]
            if tax_code == "FRE":
                if account_type == "Income":
                    return tax_code_mapping.get("FRE_Income", tax_code)
                elif account_type == "Expense":
                    return tax_code_mapping.get("FRE_Expense", tax_code)
                else:
                    return "BAS Excluded"
            elif tax_code == "GST":
                if account_type == "Income":
                    return tax_code_mapping.get("GST_Income", tax_code)
                elif account_type == "Expense":
                    return tax_code_mapping.get("GST_Expense", tax_code)
                else:
                    return "BAS Excluded"
            else:
                return tax_code_mapping.get(tax_code, tax_code)

        def map_tracking_option(row):
            matching_row = df_jobs[df_jobs["Job Number"] == row["TrackingOption1"]]
            if not matching_row.empty:
                return matching_row["Job Number"].values[0] + "-" + matching_row["Job Name"].values[0]
            else:
                return ""

        df["*TaxType"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x != "" else "")
        df["*Quantity"] = 1

        columns_order = ["*ContactName", "*InvoiceNumber", "*InvoiceDate", "*DueDate", "Reference",
                        "*Description", "*AccountCode", "*UnitAmount", "TrackingName1", "TrackingOption1",
                        "*TaxType", "TaxAmount", "*Quantity", "Currency", "Exchange Rate"]

        df['*UnitAmount'] = df['*UnitAmount'].replace({r'\$': ''}, regex=True)
        df['TaxAmount'] = df['TaxAmount'].replace({r'\$': ''}, regex=True)

        entity_name = request.session.get('entity_name', 'Default')
        safe_entity_name = entity_name.replace(" ", "_")
        date_str = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M")
        filename = f"{safe_entity_name}_Service_Invoice_{date_str}.csv"

        output_buffer = io.BytesIO()
        df[columns_order].to_csv(output_buffer, index=False, encoding='utf-8')
        output_buffer.seek(0)

        response = HttpResponse(
            content=output_buffer.getvalue(),
            content_type='text/csv'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['X-Content-Type-Options'] = 'nosniff'

        logger.info(f"Service Invoice conversion successful, returning file: {filename}")
        return response

    except Exception as e:
        logger.exception(f"Error in Service Invoice conversion: {str(e)}")
        return JsonResponse({'error': f"Service Invoice conversion failed: {str(e)}"}, status=500)

@csrf_exempt
def convert_customer(request):
    if request.method == 'POST':
        try:
            input_file = request.FILES.get("customer_file")
            if not input_file:
                logger.error("No file uploaded")
                return JsonResponse({'error': "No file uploaded. Select a CSV file."}, status=400)

            filename = input_file.name
            logger.debug(f"Processing file: {filename}")
            ext = filename.split('.')[-1].lower()
            if ext != 'csv':
                logger.error(f"Invalid file extension: {ext}")
                return JsonResponse({'error': "File must be a CSV."}, status=400)

            # Read the file
            df = read_file(input_file, filename)

            # Log DataFrame info
            logger.debug("Customer file first 3 rows:\n%s", df.head(3).to_string())

            # Handle headerless CSVs
            actual_columns = [str(col).strip().lower() for col in df.columns]
            if all(col.startswith('unnamed:') or col in ['nan', '{}'] for col in actual_columns):
                if len(df.columns) >= 2:
                    df.columns = ["First Name", "Co./Last Name"] + [f"Extra_{i}" for i in range(len(df.columns) - 2)]
                    logger.debug(f"Assigned default columns for {filename}: %s", list(df.columns))
                else:
                    logger.error(f"{filename} lacks header and has too few columns: %s", len(df.columns))
                    return JsonResponse({
                        'error': f"The file lacks a header row and has fewer than 2 columns. Expected at least: 'First Name', 'Co./Last Name'."
                    }, status=400)

            # Required columns
            required_columns = ["First Name", "Co./Last Name"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns in customer file: {missing_columns}")
                return JsonResponse({
                    'error': f"Missing required columns: {', '.join(missing_columns)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Optional columns
            optional_columns = ["Card ID", "BSB", "Account Number"]
            missing_optional = [col for col in optional_columns if col not in df.columns]
            if missing_optional:
                logger.debug(f"Optional columns missing: {missing_optional}. Adding as empty.")
                for col in missing_optional:
                    df[col] = pd.NA
                    logger.debug(f"Added empty '{col}' column")

            # Column mapping
            column_mapping = {
                "Co./Last Name": "*ContactName",
                "Card ID": "AccountNumber",
                "Addr 1 - Line 1": "POAddressLine1",
                "Addr 1 - Line 2": "POAddressLine2",
                "Addr 1 - Line 3": "POAddressLine3",
                "Addr 1 - Line 4": "POAddressLine4",
                "Addr 1 - City": "POCity",
                "Addr 1 - State": "PORegion",
                "Addr 1 - Postcode": "POPostalCode",
                "Addr 1 - Country": "POCountry",
                "Addr 1 - Phone No. 1": "PhoneNumber",
                "Addr 1 - Phone No. 2": "MobileNumber",
                "Addr 1 - Fax No.": "FaxNumber",
                "Addr 1 - Email": "EmailAddress",
                "Addr 1 - WWW": "Website",
                "Addr 1 - Salutation": "SAAttentionTo",
                "Addr 2 - Line 1": "SAAddressLine1",
                "Addr 2 - Line 2": "SAAddressLine2",
                "Addr 2 - Line 3": "SAAddressLine3",
                "Addr 2 - Line 4": "SAAddressLine4",
                "Addr 2 - City": "SACity",
                "Addr 2 - State": "SARegion",
                "Addr 2 - Postcode": "SAPostalCode",
                "Addr 2 - Country": "SACountry",
                "- % Discount": "Discount",
                "Tax ID No.": "TaxNumber",
                "Account Number": "BankAccountNumber",
                "Account Name": "BankAccountName",
                "A.B.N.": "TaxNumber",
                "Account": "SalesAccount",
                "- Balance Due Days": "DueDateSalesDay",
                "Terms - Payment is Due": "DueDateSalesTerm"
            }

            # Combine First Name and Co./Last Name into ContactName
            try:
                df["*ContactName"] = df["First Name"].fillna('') + " " + df["Co./Last Name"].fillna('')
                df["*ContactName"] = df["*ContactName"].str.strip()
                logger.debug("Created *ContactName column")
            except Exception as e:
                logger.error(f"Error creating *ContactName: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to combine 'First Name' and 'Co./Last Name': {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Combine BSB and Account Number
            try:
                df["BankAccountNumber"] = df.apply(
                    lambda row: f"{row['BSB'] if pd.notna(row['BSB']) else 'Unknown'}-{row['Account Number'] if pd.notna(row['Account Number']) else 'Unknown'}",
                    axis=1
                )
                logger.debug("Created BankAccountNumber column")
            except Exception as e:
                logger.error(f"Error creating BankAccountNumber: {str(e)}")
                df["BankAccountNumber"] = 'Unknown'
                logger.debug("Set BankAccountNumber to 'Unknown' due to error")

            # Remove First Name and Co./Last Name columns
            try:
                df = df.drop(columns=["First Name", "Co./Last Name"], errors='ignore')
                logger.debug("Dropped First Name and Co./Last Name columns")
            except Exception as e:
                logger.error(f"Error dropping columns: {str(e)}")

            # Apply column mapping
            try:
                df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
                logger.debug(f"Applied column mapping. New columns: {list(df.columns)}")
            except Exception as e:
                logger.error(f"Error applying column mapping: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to map columns: {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Keep only valid columns
            valid_columns = list(column_mapping.values()) + ["*ContactName", "BankAccountNumber"]
            try:
                df = df[[col for col in df.columns if col in valid_columns]]
                logger.debug(f"Filtered to valid columns: {list(df.columns)}")
            except Exception as e:
                logger.error(f"Error filtering columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to filter columns: {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Clean AccountNumber
            if 'AccountNumber' in df.columns:
                try:
                    df['AccountNumber'] = df['AccountNumber'].astype(str).replace(['nan', 'NaN', '*None', ''], pd.NA)
                    logger.debug(f"Cleaned AccountNumber column: {df['AccountNumber'].head().to_string()}")
                except Exception as e:
                    logger.error(f"Error cleaning AccountNumber: {str(e)}")
                    df['AccountNumber'] = pd.NA
                    logger.debug("Set AccountNumber to NA due to error")
            else:
                logger.debug("AccountNumber column not found. Adding as empty.")
                df['AccountNumber'] = pd.NA

            # Rearrange columns with ContactName first
            try:
                cols = ["*ContactName"] + [col for col in df.columns if col != "*ContactName"]
                df = df[cols]
                logger.debug(f"Rearranged columns: {list(df.columns)}")
            except Exception as e:
                logger.error(f"Error rearranging columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to rearrange columns: {str(e)}. Found columns: {', '.join(df.columns)}."
                }, status=400)

            # Generate output CSV
            entity_name = request.session.get('entity_name', 'Default').replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M")
            filename = f"{entity_name}_Customer_{date_str}.csv"

            output = io.StringIO()
            df.to_csv(output, index=False, encoding='utf-8-sig')
            csv_data = output.getvalue()

            response = HttpResponse(csv_data, content_type='text/csv; charset=utf-8')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            logger.info(f"Successfully generated Customer CSV: {filename}")
            return response

        except ValueError as ve:
            logger.error(f"ValueError in convert_customer: {str(ve)}")
            return JsonResponse({
                'error': str(ve)
            }, status=400)
        except Exception as e:
            logger.exception(f"Customer conversion error: {str(e)}")
            return JsonResponse({
                'error': f"Customer conversion failed: {str(e)}. Please verify the file is a valid CSV with required columns ('First Name', 'Co./Last Name') and optional 'Card ID' for AccountNumber. Found columns: {', '.join(df.columns) if 'df' in locals() else 'unknown'}."
            }, status=500)

    logger.error("Invalid request method: %s", request.method)
    return JsonResponse({'error': 'Use POST.'}, status=400)

@csrf_exempt
def convert_sales_invoice_product(request):
    if request.method != 'POST':
        return HttpResponse('Method not allowed', status=405)

    if not request.FILES:
        return HttpResponse('Please select a file to convert.', status=400)

    # Accept any file field name
    file_obj = next(iter(request.FILES.values()))
    filename = file_obj.name

    try:
        ext = filename.split('.')[-1].lower()
        if ext != "csv":
            return HttpResponse('Please upload a CSV file.', status=400)

        file_obj.seek(0)
        raw_content = file_obj.read().decode('utf-8', errors='ignore')
        file_obj.seek(0)

        # Try parsing with delimiters
        delimiters = [',', ';', '\t']
        df = None
        for delimiter in delimiters:
            try:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, delimiter=delimiter)
                if len(df.columns) >= 4 and not all(col.startswith('Unnamed:') for col in df.columns):
                    break
            except Exception:
                continue
        else:
            return HttpResponse("Failed to parse CSV. Ensure it contains valid headers.", status=400)

        # Normalize column names
        actual_columns = [col.strip().lower() for col in df.columns]

        # Try to map common variations
        variations = {
            "Invoice No.": ["invoice no", "invoice number", "inv no"],
            "Date": ["date", "invoice date"],
            "Customer": ["customer", "customer name"],
            "Amount": ["amount", "total"]
        }

        mapping = {}
        for std_col, aliases in variations.items():
            for alias in aliases:
                for actual in df.columns:
                    if alias.replace(" ", "").lower() in actual.replace(" ", "").lower():
                        mapping[actual] = std_col
                        break
        df.rename(columns=mapping, inplace=True)

        # Verify
        required = ["Invoice No.", "Date", "Customer", "Amount"]
        for col in required:
            if col not in df.columns:
                return HttpResponse(f"Missing required column: {col}", status=400)

        # Prepare final Xero format
        df = df[required]
        df.rename(columns={
            "Invoice No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Customer": "*ContactName",
            "Amount": "*UnitAmount"
        }, inplace=True)

        df['*DueDate'] = df['*InvoiceDate']
        df['Description'] = "Product Invoice"
        df['*Quantity'] = 1
        df['*TaxType'] = "BAS Excluded"
        df['LineAmountType'] = "Exclusive"
        df['*AccountCode'] = "200"

        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        response = HttpResponse(output.getvalue(), content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="XERO_PRODUCT_INVOICE.csv"'
        return response

    except Exception as e:
        logger.exception("Processing failed.")
        return HttpResponse(f"Error processing file: {str(e)}", status=500)


# Default function
def default_function(request):
    logger.error("Default function called - function not implemented.")
    return JsonResponse({'error': 'Function not implemented.'}, status=400)

@csrf_exempt
def upload_excel(request):
    if request.method == 'POST' and request.FILES.get('file'):
        try:
            # Save the uploaded file temporarily
            file = request.FILES['file']
            file_path = default_storage.save(file.name, file)
            full_path = os.path.join(default_storage.location, file_path)

            # Read Excel file
            df = pd.read_excel(full_path, engine='openpyxl')

            # Validate Date column
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], errors='raise')  # strict parsing
            else:
                return JsonResponse({'error': 'Missing "Date" column'}, status=400)

            # Convert to JSON
            data_json = df.to_dict(orient='records')

            # Cleanup file
            os.remove(full_path)

            return JsonResponse({'data': data_json}, safe=False)

        except ValueError as ve:
            return JsonResponse({'error': f'Date parsing error: {str(ve)}'}, status=400)

        except KeyError as ke:
            return JsonResponse({'error': f'Missing column: {str(ke)}'}, status=400)

        except Exception as e:
            return JsonResponse({'error': f'Unexpected error: {str(e)}'}, status=500)

    return JsonResponse({'error': 'Invalid request or no file provided'}, status=400)

# Upload view (optional)
def upload_file_view(request):
    if request.method == 'POST':
        uploaded_file = request.FILES['coa_file']
        file_type = uploaded_file.name.split('.')[-1].lower()

        if file_type == 'csv':
            user_encoding = request.POST.get('file_encoding')
            df = read_csv_with_encoding(uploaded_file, function_name="Uploaded File", user_encoding=user_encoding)
        elif file_type in ['xlsx', 'xls']:
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        else:
            logger.error(f"Unsupported file format in upload view: {file_type}")
            return HttpResponse("Unsupported file type", status=400)

        logger.info("File uploaded and processed successfully in upload view.")
        return HttpResponse("File uploaded and processed successfully.")
    return render(request, 'upload.html')

# Download view for converted files
def download_converted_file(request, client_id):
    try:
        # Retrieve the client object, ensuring it belongs to the authenticated user
        client = Client.objects.get(id=client_id, user=request.user)
        file_path = client.converted_file

        # Check if the file exists
        if not file_path or not os.path.exists(file_path):
            logger.error(f"File not found for client ID {client_id}: {file_path}")
            messages.error(request, "The requested file does not exist.")
            raise Http404("File not found")

        # Determine the file's content type based on its extension
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.csv':
            content_type = 'text/csv'
        elif file_extension in ['.xlsx', '.xls']:
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        else:
            logger.error(f"Unsupported file format for client ID {client_id}: {file_extension}")
            messages.error(request, "Unsupported file format.")
            raise Http404("Unsupported file format")

        # Open and read the file
        with open(file_path, 'rb') as file:
            file_data = file.read()

        # Prepare the response
        response = HttpResponse(file_data, content_type=content_type)
        filename = os.path.basename(file_path)
        response['Content-Disposition'] = f'attachment; filename="{smart_str(filename)}"'
        response['X-Content-Type-Options'] = 'nosniff'

        logger.info(f"File downloaded successfully for client ID {client_id}: {filename}")
        return response

    except Client.DoesNotExist:
        logger.error(f"Client ID {client_id} not found or does not belong to user {request.user.username}")
        messages.error(request, "Client not found or you do not have permission to access this file.")
        raise Http404("Client not found")
    except FileNotFoundError:
        logger.error(f"File not found for client ID {client_id}: {file_path}")
        messages.error(request, "The requested file could not be found on the server.")
        raise Http404("File not found")
    except Exception as e:
        logger.exception(f"Error downloading file for client ID {client_id}: {str(e)}")
        messages.error(request, f"An error occurred while downloading the file: {str(e)}")
        return redirect('dashboard')

@login_required
def entity_clients(request):
    """
    View to display clients associated with the current entity.
    """
    try:
        entity_name = request.session.get('entity_name', 'Default Company Name')
        clients = Client.objects.filter(user=request.user)
        context = {
            'entity_name': entity_name,
            'clients': clients,
        }
        logger.info(f"Displaying clients for entity: {entity_name}, user: {request.user.username}")
        return render(request, 'entity_clients.html', context)
    except Exception as e:
        logger.exception(f"Error in entity_clients view: {str(e)}")
        messages.error(request, f"An error occurred: {str(e)}")
        return redirect('dashboard')

@login_required
def my_clients(request):
    """
    View to display all clients associated with the authenticated user.
    """
    try:
        clients = Client.objects.filter(user=request.user)
        entity_name = request.session.get('entity_name', 'Default Company Name')
        context = {
            'entity_name': entity_name,
            'clients': clients,
        }
        logger.info(f"Displaying all clients for user: {request.user.username}")
        return render(request, 'my_clients.html', context)
    except Exception as e:
        logger.exception(f"Error in my_clients view: {str(e)}")
        messages.error(request, f"An error occurred: {str(e)}")
        return redirect('dashboard')

@csrf_exempt
@login_required
def convert_to_pdf(request):
    """
    View to convert client data or uploaded file to PDF.
    """
    if request.method == 'POST':
        try:
            # Example: Convert client data to PDF (modify based on your needs)
            client_id = request.POST.get('client_id')
            if not client_id:
                logger.error("No client ID provided for PDF conversion.")
                return JsonResponse({'error': 'No client ID provided.'}, status=400)

            client = Client.objects.get(id=client_id, user=request.user)
            entity_name = request.session.get('entity_name', 'Default Company Name')
            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y")
            filename = f"{safe_entity_name}_ClientReport_{date_str}.pdf"

            # Prepare context for the PDF template
            context = {
                'client': client,
                'entity_name': entity_name,
                'date': date_str,
            }

            # Render HTML template to string
            html_string = render_to_string('client_pdf.html', context)

            # Convert HTML to PDF using WeasyPrint
            pdf_file = io.BytesIO()
            HTML(string=html_string).write_pdf(pdf_file)
            pdf_data = pdf_file.getvalue()

            logger.info(f"PDF conversion successful for client ID {client_id}: {filename}")
            response = HttpResponse(pdf_data, content_type='application/pdf')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response

        except Client.DoesNotExist:
            logger.error(f"Client ID {client_id} not found for user {request.user.username}")
            return JsonResponse({'error': 'Client not found.'}, status=404)
        except Exception as e:
            logger.exception(f"Error in PDF conversion: {str(e)}")
            return JsonResponse({'error': f'PDF conversion failed: {str(e)}'}, status=500)
    else:
        # Render a form to select client or upload file for PDF conversion
        clients = Client.objects.filter(user=request.user)
        context = {
            'entity_name': request.session.get('entity_name', 'Default Company Name'),
            'clients': clients,
        }
        return render(request, 'convert_to_pdf.html', context)
    
@csrf_exempt
def convert_customer(request):
    logger.debug("convert_customer view called")
    if request.method == 'POST':
        try:
            logger.debug(f"Received files: {request.FILES.keys()}")
            input_file = request.FILES.get("customer_file")
            if not input_file:
                logger.error("No file uploaded for Customer conversion.")
                return JsonResponse({'error': "No file uploaded. Please select a valid CSV file."}, status=400)

            entity_name = request.session.get('entity_name', 'Default')
            logger.debug(f"Using entity_name: {entity_name}")
            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M")
            filename = f"{safe_entity_name}_Customer_{date_str}.csv"
            logger.debug(f"Generated filename: {filename}")

            logger.debug(f"Processing Customer file: {input_file.name}")
            if not input_file.name.lower().endswith('.csv'):
                logger.error("Unsupported file format: Only CSV is supported.")
                return JsonResponse({'error': "Unsupported file format. Please upload a CSV file."}, status=400)

            # Validate CSV format
            input_file.seek(0)
            try:
                csv.Sniffer().sniff(input_file.read(1024).decode('utf-8'))
                input_file.seek(0)
            except (csv.Error, UnicodeDecodeError) as e:
                logger.error(f"File is not a valid CSV: {str(e)}")
                return JsonResponse({
                    'error': 'Invalid CSV format. Ensure the file uses commas as separators and UTF-8 encoding.'
                }, status=400)

            # Read CSV
            logger.debug("Attempting to read CSV")
            try:
                df = read_csv_with_encoding(input_file, function_name="Customer", user_encoding=request.POST.get('file_encoding'))
            except Exception as e:
                logger.error(f"Failed to read CSV: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to read CSV: {str(e)}. Ensure the file is a valid CSV with proper encoding."
                }, status=400)

            if df is None or not isinstance(df, pd.DataFrame):
                logger.error("read_csv_with_encoding returned invalid DataFrame")
                return JsonResponse({
                    'error': "Invalid CSV data. The file could not be processed as a valid DataFrame."
                }, status=400)

            logger.debug(f"Input columns: {df.columns.tolist()}")
            df.columns = df.columns.str.strip()

            # Check required columns
            required_columns = ["First Name", "Co./Last Name"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return JsonResponse({
                    'error': f"Missing required columns: {', '.join(missing_columns)}."
                }, status=400)

            # Remove duplicate columns
            logger.debug("Removing duplicate columns")
            df = df.loc[:, ~df.columns.duplicated(keep='first')]

            # Combine First Name and Last Name
            logger.debug("Combining First Name and Co./Last Name")
            df["ContactName"] = df["First Name"].fillna('') + " " + df["Co./Last Name"].fillna('')

            # Handle BSB and Account Number
            logger.debug("Processing BSB and Account Number")
            df["BSB"] = df.get("BSB", pd.Series(['Unknown'] * len(df))).fillna('Unknown')
            df["Account Number"] = df.get("Account Number", pd.Series(['Unknown'] * len(df))).fillna('Unknown')
            df["AccountNumber"] = df.apply(
                lambda row: f"{row['BSB']}-{row['Account Number']}" if row['BSB'] != 'Unknown' and row['Account Number'] != 'Unknown' else 'Unknown',
                axis=1
            )

            # Drop unnecessary columns
            logger.debug("Dropping unnecessary columns")
            valid_columns = list(column_mapping.keys()) + ["ContactName", "AccountNumber", "BSB", "Account Number"]
            columns_to_drop = [col for col in df.columns if col not in valid_columns]
            logger.debug(f"Columns to drop: {columns_to_drop}")
            try:
                df = df.drop(columns=columns_to_drop, errors='ignore')
            except Exception as e:
                logger.error(f"Error dropping columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to process columns: {str(e)}."
                }, status=400)

            # Rename columns
            logger.debug("Renaming columns")
            try:
                df = df.rename(columns=column_mapping)
            except Exception as e:
                logger.error(f"Error renaming columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to rename columns: {str(e)}."
                }, status=400)

            df['AccountNumber'] = df['AccountNumber'].replace(["Unknown", "Unkown", "*None"], "")

            # Reorder columns
            logger.debug("Reordering columns")
            cols = ["*ContactName"] + [col for col in df.columns if col != "*ContactName"]
            try:
                df = df[cols]
            except Exception as e:
                logger.error(f"Error reordering columns: {str(e)}")
                return JsonResponse({
                    'error': f"Failed to reorder columns: {str(e)}."
                }, status=400)

            # Generate output
            logger.debug("Generating output CSV")
            output = io.StringIO()
            df.to_csv(output, index=False, encoding='ascii', errors='ignore')
            csv_data = output.getvalue()
            output.close()
            logger.debug(f"Output CSV size: {len(csv_data)} bytes")

            logger.info(f"Customer conversion successful: {filename}")
            response = HttpResponse(csv_data, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response

        except ValueError as ve:
            logger.exception(f"Encoding error in Customer conversion: {str(ve)}")
            return JsonResponse({
                'error': (
                    f"{str(ve)} Try the following: "
                    "1. Open the file in Notepad++ or VS Code. "
                    "2. Save it as CSV with UTF-8 or ISO-8859-1 encoding. "
                    "3. Ensure it uses commas (,) as separators and has valid headers."
                )
            }, status=400)
        except Exception as e:
            logger.exception(f"Error in Customer conversion: {str(e)}")
            return JsonResponse({
                'error': (
                    f"Customer conversion failed: {str(e)}. Please verify the file is a valid CSV with the required columns "
                    "and check the server logs for details."
                )
            }, status=500)

    return JsonResponse({'error': 'Invalid request method. Please use POST.'}, status=400)


def main_view(request):
    entity_name = request.session.get('entity_name', '')
    print(f"Main view - Entity name from session: {entity_name}")  # Debugging
    return render(request, 'main.html', {'entity_name': entity_name})

def myob_xero_view(request):
    entity_name = request.session.get('entity_name', '')
    print(f"Myob-xero view - Entity name from session: {entity_name}")  # Debugging
    return render(request, 'myob-xero.html', {'entity_name': entity_name})


@csrf_exempt
def convert_open_ap(request):
    """
    Convert Open AP data from MYOB to Xero format.
    """
    logger.debug("convert_open_ap called")
    if request.method != 'POST':
        return JsonResponse({'error': 'Use POST.'}, status=400)

    try:
        open_ap_file = request.FILES.get("open_ap_file")
        if not open_ap_file:
            logger.error("No file uploaded for Open AP")
            return JsonResponse({'error': "No file uploaded. Please select a valid CSV or Excel file."}, status=400)

        if not open_ap_file.name.lower().endswith(('.csv', '.xlsx', '.xls')):
            logger.error(f"Invalid file format: {open_ap_file.name}")
            return JsonResponse({'error': f"Invalid file format: {open_ap_file.name}. File must be a CSV or Excel file."}, status=400)

        df = pd.read_csv(open_ap_file) if open_ap_file.name.endswith('.csv') else pd.read_excel(open_ap_file)
        df.dropna(how='all', inplace=True)
        df.columns = df.columns.str.strip()
        logger.debug(f"Initial columns: {df.columns.tolist()}")
        logger.debug(f"Initial row count: {len(df)}")
        logger.debug(f"Sample rows:\n{df.head(5).to_string()}")

        # Check for header misalignment
        potential_headers = [
            "ID No.", "Invoice Number", "Date", "Invoice Date", "Created", 
            "Total Due", "Amount", "Balance Due", "Customer", "Currency"
        ]
        first_row = df.iloc[0].astype(str).str.strip()
        if any(first_row.str.contains('|'.join(potential_headers), case=False, na=False, regex=True)):
            logger.info("Detected potential header row in data; reassigning headers")
            df.columns = first_row
            df = df.drop(0).reset_index(drop=True)
            df.columns = df.columns.str.strip()
            logger.debug(f"New columns after header reassignment: {df.columns.tolist()}")

        # Skip metadata rows
        metadata_patterns = [
            r'PO Box', r'ABN:', r'Reconciliation', r'Transactions shown', 
            r'Total', r'Summary', r'Report', r'As of', r'Email:', 
            r'Phone:', r'Fax:', r'Website:', r'Inc\s', r'Ltd\s', r'Group\s'
        ]
        is_metadata = df.iloc[:, 0].astype(str).str.contains(
            '|'.join(metadata_patterns), case=False, na=False, regex=True
        ) | df.iloc[:, 0].astype(str).str.strip().isin(df.columns)
        df = df[~is_metadata]
        logger.debug(f"Row count after skipping metadata: {len(df)}")

        # Expanded column name matching
        column_variations = {
            "Date": [
                "Date", "Invoice Date", "Created Date", "Due Date", "Transaction Date", 
                "Posting Date", "Created", "Created:.*"
            ],
            "Total Due": [
                "Total Due", "Amount", "Total", "Balance Due", "Amount Due", 
                "Outstanding", "Balance", "Invoice Amount", "Due", "Total Amount"
            ],
            "ID No.": [
                "ID No.", "Invoice Number", "Invoice No", "Inv No", "Bill No", "Reference"
            ],
            "Orig. Curr.": [
                "Orig. Curr.", "Currency", "Currency Code", "Curr"
            ]
        }

        column_mapping = {}
        for required, variants in column_variations.items():
            for col in df.columns:
                for variant in variants:
                    if col == variant or re.match(variant, col, re.IGNORECASE):
                        column_mapping[col] = required
                        break
                if col in column_mapping:
                    break

        # Check for missing mandatory columns
        missing_columns = [req for req in ["Date", "Total Due"] if req not in column_mapping.values()]
        if missing_columns:
            if "Total Due" in missing_columns:
                candidate_cols = [col for col in df.columns if col not in column_mapping]
                for col in candidate_cols:
                    try:
                        temp_series = pd.to_numeric(
                            df[col].astype(str).str.replace(r'[\$,\sA-Z]+', '', regex=True),
                            errors='coerce'
                        )
                        if temp_series.notna().sum() > 0 and (temp_series >= 0).any():
                            df[col] = temp_series
                            column_mapping[col] = "Total Due"
                            missing_columns = [req for req in missing_columns if req != "Total Due"]
                            logger.info(f"Inferred 'Total Due' as column: {col}")
                            break
                    except Exception as e:
                        logger.debug(f"Failed to convert {col} to numeric: {str(e)}")

            if "Total Due" in missing_columns:
                logger.warning("No Total Due column found; assigning default value 0")
                df["Total Due"] = 0
                column_mapping["Total Due"] = "Total Due"
                missing_columns = [req for req in missing_columns if req != "Total Due"]

            if missing_columns:
                error_msg = (
                    f"Missing required columns: {', '.join(missing_columns)}. "
                    f"Found columns: {', '.join(df.columns)}. "
                    f"Please ensure columns like 'Date' (e.g., 'Invoice Date', 'Created') and "
                    f"'Total Due' (e.g., 'Amount', 'Balance Due', 'Total') are present."
                )
                logger.error(error_msg)
                return JsonResponse({'error': error_msg}, status=400)

        df.rename(columns=column_mapping, inplace=True)

        # Log sample data for all columns
        for col in df.columns:
            sample_values = df[col].head(5).tolist()
            logger.debug(f"Sample values in {col}: {sample_values}")

        # Preprocess Date column
        def is_valid_date(value):
            try:
                pd.to_datetime(value, errors='raise')
                return True
            except (ValueError, TypeError):
                return False

        # Filter valid dates
        if 'Date' in df.columns:
            df['Date_Is_Valid'] = df['Date'].apply(is_valid_date)
            invalid_dates = df[~df['Date_Is_Valid']]['Date'].dropna().head(3).tolist()
            df = df[df['Date_Is_Valid']].drop(columns=['Date_Is_Valid'])
            logger.debug(f"Row count after date validation: {len(df)}")
            if invalid_dates:
                logger.warning(f"Invalid date values filtered: {invalid_dates}")

        # Validate date format
        try:
            df['Date'] = pd.to_datetime(df['Date'], errors='raise')
        except Exception as e:
            logger.error(f"Error converting 'Date' column: {str(e)}")
            return JsonResponse({
                'error': (
                    f"Invalid date format in 'Date' column: {str(e)}. "
                    f"Ensure dates are in a recognizable format (e.g., YYYY-MM-DD, DD-MMM-YY). "
                    f"Found non-date values like: {invalid_dates[:3]}. "
                    f"Please remove non-date data (e.g., customer names, invoice numbers) from the Date column."
                )
            }, status=400)

        # Filter valid transaction rows
        df = df[df['Date'].notna() & df['Total Due'].notna()]
        logger.debug(f"Row count after Date and Total Due filtering: {len(df)}")
        if df.empty:
            sample_dates = df['Date'].head(3).tolist() if 'Date' in df.columns else ['No Date column']
            sample_totals = df['Total Due'].head(3).tolist() if 'Total Due' in df.columns else ['No Total Due column']
            error_msg = (
                "No valid transaction rows found with non-null Date and Total Due. "
                "Possible issues: "
                "1) The Date column contains only invalid or missing values. "
                f"Sample Date values: {sample_dates}. "
                "2) The Total Due column is missing or contains no valid amounts. "
                f"Sample Total Due values: {sample_totals}. "
                "Please ensure the file contains transaction rows with valid dates (e.g., '2023-10-05') "
                "and amounts (e.g., '300.00'). Remove metadata like customer names, invoice numbers, "
                "or misplaced headers. Check if the header row is misaligned with data."
            )
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=400)

        # Add *ContactName column
        current_customer = None
        customer_names = []

        def is_valid_customer_name(name):
            return bool(re.match(r'^[A-Za-z\s\(\)]+$', str(name))) and len(str(name).split()) > 1

        for idx, row in df.iterrows():
            first_col = row.iloc[0]
            non_empty_cols = row.notna().sum()
            if non_empty_cols == 1 and pd.notna(first_col) and not str(first_col).startswith(("*", "Total", "Grand")) and is_valid_customer_name(first_col):
                current_customer = str(first_col).strip()
            customer_names.append(current_customer)

        df['*ContactName'] = customer_names

        # Map to output format
        output_mapping = {
            "ID No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Orig. Curr.": "Currency",
            "Total Due": "*UnitAmount"
        }
        df.rename(columns=output_mapping, inplace=True)

        # Keep only needed columns
        required_output_columns = list(output_mapping.values()) + ['*ContactName']
        df = df[[col for col in required_output_columns if col in df.columns]]

        # Add extra columns
        df['*DueDate'] = df['*InvoiceDate']
        df['Description'] = "."
        df['*Quantity'] = 1
        df["*TaxType"] = "BAS Excluded"
        df['LineAmountType'] = "Exclusive"
        df["*AccountCode"] = "960"

        df = df[df['*InvoiceNumber'].notna()]
        logger.debug(f"Row count after InvoiceNumber filtering: {len(df)}")
        if df.empty:
            logger.error("No rows with valid InvoiceNumber found after filtering")
            return JsonResponse({'error': "No rows with valid InvoiceNumber found."}, status=400)

        # Generate output file
        entity_name = request.session.get('entity_name', 'Default').replace(" ", "_")
        date_str = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M")
        filename = f"{entity_name}_OpenAP_{date_str}.csv"

        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        csv_data = output.getvalue()

        logger.info(f"Open AP converted: {filename}")
        response = HttpResponse(csv_data, content_type='text/csv; charset=utf-8')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response

    except ValueError as ve:
        logger.exception(f"Open AP processing error: {str(ve)}")
        return JsonResponse({'error': str(ve)}, status=400)
    except Exception as e:
        logger.exception(f"Unexpected error during Open AP processing: {str(e)}")
        return JsonResponse({'error': "An unexpected error occurred. Please try again later."}, status=500)
    
def convert_open_ar(request):
    if request.method != 'POST':
        logger.error("Invalid request method: %s", request.method)
        return HttpResponse('Method not allowed', status=405)

    # Check if file is uploaded
    if 'open_ar_file' not in request.FILES:
        logger.error("No file uploaded in request")
        return HttpResponse('No file uploaded', status=400)

    file_obj = request.FILES['open_ar_file']
    filename = file_obj.name
    logger.debug("Processing file: %s", filename)

    try:
        # Validate file extension
        ext = filename.split('.')[-1].lower()
        if ext not in ["csv", "xls", "xlsx"]:
            logger.error("Unsupported file extension: %s", ext)
            return HttpResponse(f"Unsupported file extension: {ext}. Please upload a CSV or Excel file.", status=400)

        # Read the file
        if ext == "csv":
            try:
                temp_df = pd.read_csv(file_obj, encoding='utf-8', header=None)
            except UnicodeDecodeError:
                logger.debug("UTF-8 encoding failed, trying ISO-8859-1")
                file_obj.seek(0)
                temp_df = pd.read_csv(file_obj, encoding='ISO-8859-1', header=None)
            except pd.errors.EmptyDataError:
                logger.error("Uploaded CSV file is empty")
                return HttpResponse("The uploaded CSV file is empty.", status=400)
        else:
            try:
                temp_df = pd.read_excel(file_obj, header=None)
            except ValueError:
                logger.error("Invalid Excel file format")
                return HttpResponse("The uploaded Excel file is invalid or corrupted.", status=400)

        # Validate file content
        if temp_df.empty:
            logger.error("File contains no data")
            return HttpResponse("The uploaded file contains no data.", status=400)

        # Detect header row based on 'ID No.'
        header_row = temp_df[temp_df.apply(lambda row: row.astype(str).str.contains('ID No.', na=False)).any(axis=1)]
        if header_row.empty:
            logger.error("Header row with 'ID No.' not found")
            return HttpResponse("The file does not contain a header row with 'ID No.'", status=400)
        
        header_row_idx = header_row.index[0]
        logger.debug("Header row detected at index: %d", header_row_idx)

        # Reload the file with the correct header
        file_obj.seek(0)
        if ext == "csv":
            try:
                df = pd.read_csv(file_obj, skiprows=header_row_idx)
            except pd.errors.EmptyDataError:
                logger.error("No data after skipping header rows")
                return HttpResponse("No valid data found after the header row.", status=400)
        else:
            df = pd.read_excel(file_obj, skiprows=header_row_idx)

        # Validate required columns
        required_columns = ["ID No.", "Date", "Orig. Curr.", "Total Due"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error("Missing required columns: %s", missing_columns)
            return HttpResponse(f"Missing required columns: {', '.join(missing_columns)}", status=400)

        # Define column mappings
        column_mapping = {
            "ID No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Orig. Curr.": "Currrency",
            "Total Due": "*UnitAmount"
        }

        # Store a copy before renaming
        df_original = df.copy()

        # --- Step 1: Add *ContactName column ---
        current_customer = None
        customer_names = []

        def is_valid_customer_name(name):
            return bool(re.match(r'^[A-Za-z\s]+$', name)) and len(name.split()) > 1

        for idx, row in df.iterrows():
            non_empty_cols = row.notna().sum()
            first_col = row.iloc[0]
            if non_empty_cols == 1 and pd.notna(first_col) and not str(first_col).startswith(("*", "Total", "Grand")):
                if isinstance(first_col, str) and is_valid_customer_name(first_col):
                    current_customer = str(first_col).strip()
            customer_names.append(current_customer)

        df['*ContactName'] = customer_names

        # --- Step 2: Filter only valid transaction rows ---
        df = df[df['Date'].notna() & df['Total Due'].notna()]
        if df.empty:
            logger.error("No valid transaction rows found after filtering")
            return HttpResponse("No valid transaction rows found in the file.", status=400)

        # --- Step 3: Rename columns ---
        df.rename(columns=column_mapping, inplace=True)

        # --- Step 4: Keep only needed columns ---
        df = df[list(column_mapping.values()) + ['*ContactName']]

        # --- Step 5: Add extra columns ---
        df['*DueDate'] = df['*InvoiceDate']
        df['Description'] = "."
        df['*Quantity'] = 1
        df["*TaxType"] = "BAS Excluded"
        df['LineAmountType'] = "Exclusive"
        df["*AccountCode"] = "960"

        df = df[df['*InvoiceNumber'].notna()]
        if df.empty:
            logger.error("No rows with valid InvoiceNumber after final filtering")
            return HttpResponse("No rows with valid InvoiceNumber found.", status=400)

        # Create CSV response
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        response = HttpResponse(
            content=output.getvalue(),
            content_type='text/csv'
        )
        response['Content-Disposition'] = 'attachment; filename="XERO_OPEN_AR.csv"'
        logger.info("Successfully generated CSV response")
        return response

    except Exception as e:
        logger.exception("Error processing file: %s", str(e))
        return HttpResponse(f"Error processing file: {str(e)}", status=500)

def convert_payroll_journal(request):
    pass

# Convert Purchase Bill Serevice

# Read file function with debug output and error handling
def read_file(file_obj, filename):
    ext = filename.split('.')[-1].lower()
    allowed_extensions = ['csv', 'xls', 'xlsx', 'txt']
    
    if ext not in allowed_extensions:
        raise ValueError(f"Unsupported file extension: {ext}. Allowed: {', '.join(allowed_extensions)}")

    try:
        if ext == "csv":
            try:
                df = pd.read_csv(file_obj, encoding='utf-8', delimiter=',')
            except UnicodeDecodeError:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, encoding='ISO-8859-1', delimiter=',')
            except Exception as e:
                raise ValueError(f"Failed to read CSV file {filename}: {str(e)}")
        elif ext in ["xls", "xlsx"]:
            df = pd.read_excel(file_obj)
        elif ext == "txt":
            try:
                df = pd.read_csv(file_obj, delimiter='\t', encoding='utf-8')
            except UnicodeDecodeError:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, delimiter='\t', encoding='ISO-8859-1')
            except Exception as e:
                raise ValueError(f"Failed to read TXT file {filename}: {str(e)}")
        
        df.columns = df.columns.str.strip()
        print(f"Columns in {filename}: {list(df.columns)}")  # Debug print
        return df
    except Exception as e:
        raise ValueError(f"Error processing file {filename}: {str(e)}")


def read_file(file_obj, filename):
    ext = filename.split('.')[-1].lower()
    if ext == "csv":
        try:
            return pd.read_csv(file_obj, encoding='utf-8')
        except UnicodeDecodeError:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding='ISO-8859-1')
    elif ext in ["xls", "xlsx"]:
        return pd.read_excel(file_obj)
    elif ext == "txt":
        try:
            return pd.read_csv(file_obj, delimiter='\t', encoding='utf-8')
        except UnicodeDecodeError:
            file_obj.seek(0)
            return pd.read_csv(file_obj, delimiter='\t', encoding='ISO-8859-1')
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

def convert_purchase_bill_service(request):
    if request.method == 'POST':
        service_file = request.FILES.get('purchase_bill_service_file')
        coa_file = request.FILES.get('coa_file_service')
        job_file = request.FILES.get('job_file_service')

        if not all([service_file, coa_file, job_file]):
            return HttpResponse("All three files are required.", status=400)

        df = read_file(service_file, service_file.name)
        df_coa = read_file(coa_file, coa_file.name)
        df_jobs = read_file(job_file, job_file.name)

        df.dropna(how='all', inplace=True)
        df.columns = df.columns.str.strip()
        df_coa.columns = df_coa.columns.str.strip()
        df_jobs.columns = df_jobs.columns.str.strip()

        required_columns = [
            'Co./Last Name', 'Purchase No.', 'Date', '- Balance Due Days',
            'Description', 'Account No.', 'Amount', 'Job', 'Tax Code',
            'Tax Amount', 'Currency Code', 'Exchange Rate'
        ]
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""

        column_mapping = {
            "Co./Last Name": "*ContactName",
            "Purchase No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "- Balance Due Days": "*DueDate",
            "Description": "*Description",
            "Account No.": "*AccountCode",
            "Amount": "*UnitAmount",
            "Job": "TrackingOption1",
            "Tax Code": "*TaxType",
            "Tax Amount": "TaxAmount",
            "Currency Code": "Currency",
            "Exchange Rate": "Exchange Rate"
        }
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        if "First Name" in df.columns:
            df["*ContactName"] = df["First Name"].fillna('') + " " + df["*ContactName"].fillna('')
        df["*ContactName"] = df["*ContactName"].fillna('').str.strip()

        df["*DueDate"] = df["*InvoiceDate"]
        df["*Description"] = df["*Description"].fillna(".")
        df["*Quantity"] = 1

        tax_code_mapping = {
            "CAP": "GST on Capital",
            "FRE_Expense": "GST Free Expenses",
            "FRE_Income": "GST Free Income",
            "GST_Expense": "GST on Expenses",
            "GST_Income": "GST on Income",
            "IMP": "GST on Imports",
            "INP": "Input Taxed",
            "N-T": "BAS Excluded",
            "ITS": "BAS Excluded",
            "EXP": "BAS Excluded",
            "": "BAS Excluded"
        }

        def map_tax_code(row):
            account_code = row.get("*AccountCode", "")
            tax_code = row.get("*TaxType", "")
            if account_code is None or not str(account_code).strip():
                return tax_code_mapping.get(tax_code, "BAS Excluded")
            try:
                account_code = str(int(float(account_code)))
            except:
                return tax_code_mapping.get(tax_code, "BAS Excluded")
            coa_row = df_coa[df_coa["Account Number"].astype(str) == account_code]
            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")
            acc_type = coa_row.iloc[0]["Account Type"]
            if tax_code == "FRE":
                return tax_code_mapping.get("FRE_Income" if acc_type == "Income" else "FRE_Expense", "BAS Excluded")
            elif tax_code == "GST":
                return tax_code_mapping.get("GST_Income" if acc_type == "Income" else "GST_Expense", "BAS Excluded")
            return tax_code_mapping.get(tax_code, "BAS Excluded")

        def map_tracking_option(row):
            match = df_jobs[df_jobs["Job Number"] == row.get("TrackingOption1", "")]
            return match["Job Number Xero"].values[0] if not match.empty else ""

        df["*TaxType"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x else "")

        columns_order = ["*ContactName", "*InvoiceNumber", "*InvoiceDate", "*DueDate",
                         "*Description", "*AccountCode", "*UnitAmount", "TrackingName1", "TrackingOption1",
                         "*TaxType", "TaxAmount", "*Quantity", "Currency", "Exchange Rate"]

        df_final = df[columns_order]

        # Save to in-memory CSV
        output = io.StringIO()
        df_final.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)

        # Return as downloadable file
        response = HttpResponse(output, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=XERO_PURCHASE_BILL_SERVICE.csv'
        return response
    

def convert_purchase_bill_product(request):
    if request.method != 'POST':
        return HttpResponse(
            json.dumps({'error': 'Invalid request method'}),
            content_type='application/json',
            status=405
        )

    try:
        # Retrieve uploaded files
        invoice_file = request.FILES.get('purchase_bill_product_file')
        coa_file = request.FILES.get('coa_file_product')
        item_file = request.FILES.get('item_file_product')
        job_file = request.FILES.get('job_file_product')

        if not all([invoice_file, coa_file, item_file, job_file]):
            return HttpResponse(
                json.dumps({'error': 'Please upload Purchase Invoice Product, COA, Item, and Job files'}),
                content_type='application/json',
                status=400
            )

        # Validate file extensions
        if not (invoice_file.name.endswith(('.csv', '.xlsx', '.xls')) and
                coa_file.name.endswith('.csv') and
                item_file.name.endswith('.csv') and
                job_file.name.endswith('.csv')):
            return HttpResponse(
                json.dumps({'error': 'Purchase Invoice Product must be CSV/Excel, others must be CSV'}),
                content_type='application/json',
                status=400
            )

        # Read files
        df = read_file(invoice_file, invoice_file.name)
        df_coa = read_file(coa_file, coa_file.name)
        df_item = read_file(item_file, item_file.name)
        df_jobs = read_file(job_file, job_file.name)

        # Clean column names
        df.columns = df.columns.str.strip()
        df_coa.columns = df_coa.columns.str.strip()
        df_item.columns = df_item.columns.str.strip()
        df_jobs.columns = df_jobs.columns.str.strip()

        # Drop empty rows
        df.dropna(how='all', inplace=True)

        # Column mapping
        column_mapping = {
            "ContactName": "*ContactName",
            "Purchase No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Item Number": "InventoryItemCode",
            "Quantity": "*Quantity",
            "Description": "*Description",
            "Price": "*UnitAmount",
            "Job": "TrackingOption1",
            "Tax Code": "*TaxType",
            "Tax Amount": "Tax Amount",
            "Currency Code": "Currency",
            "Exchange Rate": "Exchange Rate"
        }

        # Combine First Name and Last Name
        df["ContactName"] = df["First Name"].fillna("") + " " + df["Co./Last Name"].fillna("")

        # Rename columns
        df.rename(columns={col: column_mapping[col] for col in df.columns if col in column_mapping}, inplace=True)

        # Set DueDate
        df["*DueDate"] = df["*InvoiceDate"]

        # Fill empty descriptions
        df["*Description"] = df["*Description"].fillna(".")

        # Tax code mapping
        tax_code_mapping = {
            "CAP": "GST on Capital",
            "FRE_Expense": "GST Free Expenses",
            "FRE_Income": "GST Free Income",
            "GST_Expense": "GST on Expenses",
            "GST_Income": "GST on Income",
            "IMP": "GST on Imports",
            "INP": "Input Taxed",
            "N-T": "BAS Excluded",
            "ITS": "BAS Excluded",
            "EXP": "BAS Excluded",
            "": "BAS Excluded"
        }

        def map_tax_code(row):
            if "*AccountCode" not in row or "*TaxType" not in row:
                return "BAS Excluded"

            account_code = str(int(float(row["*AccountCode"]))) if pd.notna(row["*AccountCode"]) else ""
            tax_code = row["*TaxType"]

            df_coa["Account Number"] = df_coa["Account Number"].astype(str)
            coa_row = df_coa[df_coa["Account Number"] == account_code]

            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            account_type = coa_row.iloc[0]["Account Type"]

            if tax_code == "FRE":
                if account_type == "Income":
                    return tax_code_mapping.get("FRE_Income", tax_code)
                elif account_type == "Expense":
                    return tax_code_mapping.get("FRE_Expense", tax_code)
                else:
                    return "BAS Excluded"
            elif tax_code == "GST":
                if account_type == "Income":
                    return tax_code_mapping.get("GST_Income", tax_code)
                elif account_type == "Expense":
                    return tax_code_mapping.get("GST_Expense", tax_code)
                else:
                    return "BAS Excluded"

            return tax_code_mapping.get(tax_code, tax_code)

        # Map account code based on inventory
        def map_account_code(item_number):
            row = df_item[df_item["Item Number"] == item_number]
            if not row.empty:
                if pd.notna(row.iloc[0]["Inventory"]) and str(row.iloc[0]["Inventory"]).strip() != "":
                    return row.iloc[0]["Asset Acct"]
                else:
                    return row.iloc[0]["Income Acct"]
            return None

        df["*AccountCode"] = df["InventoryItemCode"].apply(map_account_code)
        df["*TaxType"] = df.apply(map_tax_code, axis=1)

        # Tracking mapping
        def map_tracking_option(row):
            match = df_jobs[df_jobs["Job Number"] == row["TrackingOption1"]]
            if not match.empty:
                return match.iloc[0]["Job Number Xero"]
            return ""

        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x != "" else "")

        # Ensure numeric columns
        df["*Quantity"] = pd.to_numeric(df["*Quantity"], errors="coerce")
        df["*UnitAmount"] = pd.to_numeric(df["*UnitAmount"], errors="coerce")

        # Handle negative quantities
        mask = df["*Quantity"] < 0
        df.loc[mask, "*Quantity"] = 4
        df.loc[mask, "*UnitAmount"] = -df.loc[mask, "*UnitAmount"]

        # Column order
        columns_order = [
            "*ContactName", "*InvoiceNumber", "*InvoiceDate", "*DueDate",
            "InventoryItemCode", "*AccountCode", "*Quantity", "*Description", "*UnitAmount",
            "TrackingName1", "TrackingOption1", "*TaxType", "Tax Amount", "Currency", "Exchange Rate"
        ]

        # Ensure all columns exist
        for col in columns_order:
            if col not in df.columns:
                df[col] = ""

        # Create output Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df[columns_order].to_excel(writer, index=False)
        output.seek(0)

        # Prepare response
        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': 'attachment; filename="XERO_PURCHASE_BILL_PRODUCT.xlsx"'}
        )
        response.write(output.getvalue())
        return response

    except Exception as e:
        return HttpResponse(
            json.dumps({'error': str(e)}),
            content_type='application/json',
            status=500
        )
        

def convert_invoice_payment(request):
    if request.method != 'POST':
        return HttpResponse(
            json.dumps({'error': 'Invalid request method'}),
            content_type='application/json',
            status=405
        )

    try:
        payment_file = request.FILES.get('invoice_payment_file')

        if not payment_file:
            return HttpResponse(
                json.dumps({'error': 'Please upload an Invoice Payment file'}),
                content_type='application/json',
                status=400
            )

        if not payment_file.name.endswith(('.csv', '.xlsx', '.xls')):
            return HttpResponse(
                json.dumps({'error': 'Invoice Payment file must be CSV or Excel'}),
                content_type='application/json',
                status=400
            )

        df = read_file(payment_file, payment_file.name)

        column_mapping = {
            'date1': 'Date',
            'bankcode': 'Bank',
            'Ref': 'Reference',
            'amount': 'Amount',
            'INVNO': 'Invoice No',
            'Exchange': 'Exchange'
        }

        df = df.rename(columns=column_mapping)

        # Clean and format data
        if 'Date' in df.columns:
            df['Date'] = df['Date'].astype(str).str.extract(r'DatD:(.*)')
        if 'Bank' in df.columns:
            df['Bank'] = df['Bank'].astype(str).str.extract(r'Code-(\d)-(\d+)$').agg(''.join, axis=1)
        if 'Reference' in df.columns:
            df['Reference'] = [f"{val}-{i+1}" for i, val in enumerate(df['Reference'].astype(str))]

        if 'Exchange' not in df.columns:
            df['Exchange'] = ''

        mandatory_columns = ['Date', 'Bank', 'Reference', 'Amount', 'Invoice No', 'Exchange']
        missing_columns = [col for col in mandatory_columns if col not in df.columns]
        if missing_columns:
            return HttpResponse(
                json.dumps({'error': f"Missing mandatory columns: {', '.join(missing_columns)}"}),
                content_type='application/json',
                status=400
            )

        df = df[mandatory_columns]

        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)

        response = HttpResponse(
            content_type='text/csv',
            headers={'Content-Disposition': 'attachment; filename="XERO_INVOICE_PAYMENT.csv"'}
        )
        response.write(output.getvalue())
        return response

    except Exception as e:
        return HttpResponse(
            json.dumps({'error': str(e)}),
            content_type='application/json',
            status=500
        )

def convert_bill_payment(request):
    if request.method != 'POST':
        return HttpResponse(
            json.dumps({'error': 'Invalid request method'}),
            content_type='application/json',
            status=405
        )

    try:
        payment_file = request.FILES.get('bill_payment_file')

        if not payment_file:
            return HttpResponse(
                json.dumps({'error': 'Please upload a Bill Payment file'}),
                content_type='application/json',
                status=400
            )

        if not payment_file.name.endswith(('.csv', '.xlsx', '.xls')):
            return HttpResponse(
                json.dumps({'error': 'Bill Payment file must be CSV or Excel'}),
                content_type='application/json',
                status=400
            )

        def read_file(file, filename):
            if filename.endswith('.csv'):
                return pd.read_csv(file)
            else:
                return pd.read_excel(file)

        df = read_file(payment_file, payment_file.name)

        # Flexible column mapping
        column_variations = {
            'Date': ['date1', 'Date', 'Payment Date'],
            'Bank': ['bankcode', 'Bank', 'Bank Code'],
            'Reference': ['Ref', 'Reference', 'Payment Ref'],
            'Amount': ['amount', 'Amount', 'Total'],
            'Invoice No': ['billNO', 'Invoice No', 'Bill No', 'Invoice Number'],
            'Exchange': ['Exchange', 'Exchange Rate', 'Rate']
        }

        column_mapping = {}
        for standard_col, variations in column_variations.items():
            for col in df.columns:
                clean_col = col.strip()
                if clean_col in variations or any(v.lower() in clean_col.lower() for v in variations):
                    column_mapping[col] = standard_col
                    break

        df = df.rename(columns=column_mapping)

        # Ensure required columns exist
        required_cols = ['Date', 'Bank', 'Reference', 'Amount', 'Invoice No']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            return HttpResponse(
                json.dumps({'error': f"Missing mandatory columns: {', '.join(missing)}. Found columns: {', '.join(df.columns)}"}),
                content_type='application/json',
                status=400
            )

        # Data cleaning
        df['Date'] = df['Date'].astype(str).str.extract(r'DatD:(.*)', expand=False).fillna(df['Date'])
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')

        df['Bank'] = df['Bank'].astype(str).str.extract(r'Code-(\d)-(\d+)$').agg(''.join, axis=1).fillna(df['Bank'])
        df['Reference'] = [f"{val}-{i+1}" for i, val in enumerate(df['Reference'].astype(str))]

        if 'Exchange' not in df.columns:
            df['Exchange'] = ''

        df = df[['Date', 'Bank', 'Reference', 'Amount', 'Invoice No', 'Exchange']]

        # Export as CSV
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)

        response = HttpResponse(
            output.getvalue(),
            content_type='text/csv'
        )
        response['Content-Disposition'] = 'attachment; filename="XERO_BILL_PAYMENT.csv"'
        return response

    except Exception as e:
        return HttpResponse(
            json.dumps({'error': str(e)}),
            content_type='application/json',
            status=500
        )
        
tax_code_mapping = {
    "GST_Expense": "GST on Expenses",
    "GST_Income": "GST on Income",
    "FRE_Expense": "GST Free Expenses",
    "FRE_Income": "GST Free Income",
    "": "BAS Excluded",
    "GST": "GST on Expenses",
    "FRE": "GST Free Expenses",
    "N-T": "BAS Excluded"
}

@csrf_exempt
def convert_receive_money(request):
    if request.method != "POST":
        return HttpResponse("Method not allowed", status=405)

    # Check for required files
    required_files = ['csv_file', 'coa_file', 'job_file']
    missing_files = [f for f in required_files if f not in request.FILES]
    if missing_files:
        return HttpResponse(f"Missing required files: {', '.join(missing_files)}", status=400)

    # Retrieve files
    csv_file = request.FILES['csv_file']
    coa_file = request.FILES['coa_file']
    job_file = request.FILES['job_file']

    # Define column mappings
    column_mapping = {
        "Deposit Account": "Bank",
        "ID No.": "Reference",
        "Date": "Date",
        "Co./Last Name": "Payee",
        "Memo": "Description",
        "Allocation Account No.": "Account Code",
        "Amount": "Amount",
        "Job No.": "Toption",
        "Tax Code": "Tax",
        "Tax Amount": "Tax Amount",
        "Currency Code": "Currency Name",
        "Exchange Rate": "Currency rate",
        "": "Line Amount Type"
    }

    try:
        # Read the CSV file
        def read_file(file, filename, is_main_csv=False):
            ext = filename.split('.')[-1].lower()
            try:
                content = file.read().decode('utf-8-sig', errors='ignore')
                delimiter = ',' if ',' in content.split('\n')[0] else ';'
                df = pd.read_csv(io.StringIO(content), delimiter=delimiter)
                if is_main_csv:
                    logger.debug(f"Initial CSV columns: {df.columns}")
                    df = df.iloc[:, :len(column_mapping)]  # Adjust to expected columns if too many
                    df.columns = list(column_mapping.keys())
                return df
            except Exception as e:
                logger.error(f"Error reading file {filename}: {str(e)}")
                raise

        # Read the files
        df = read_file(csv_file, csv_file.name, is_main_csv=True)
        df_coa = read_file(coa_file, coa_file.name)
        df_jobs = read_file(job_file, job_file.name)

        # Log columns of the dataframe to help debug
        logger.debug(f"Columns in the CSV file: {df.columns.tolist()}")

        # Check for variations in the Payee column name
        payee_column_found = False
        payee_column_candidates = ['Payee', 'Co./Last Name', 'Co. / Last Name', 'Last Name']

        for candidate in payee_column_candidates:
            if candidate in df.columns:
                logger.debug(f"Found Payee column under: {candidate}")
                df.rename(columns={candidate: 'Payee'}, inplace=True)
                payee_column_found = True
                break

        if not payee_column_found:
            logger.error("The 'Payee' column is missing in the input CSV.")
            return HttpResponse("The 'Payee' column is missing or named differently in the input CSV.", status=400)

        # Check for variations in the Description column name
        description_column_found = False
        description_column_candidates = ['Description', 'Memo', 'Details', 'Comments']

        for candidate in description_column_candidates:
            if candidate in df.columns:
                logger.debug(f"Found Description column under: {candidate}")
                df.rename(columns={candidate: 'Description'}, inplace=True)
                description_column_found = True
                break

        if not description_column_found:
            logger.error("The 'Description' column is missing in the input CSV.")
            return HttpResponse("The 'Description' column is missing or named differently in the input CSV.", status=400)

        # Check for variations in the Tax column name
        tax_column_found = False
        tax_column_candidates = ['Tax', 'Tax Code', 'GST Code', 'Tax Type']

        for candidate in tax_column_candidates:
            if candidate in df.columns:
                logger.debug(f"Found Tax column under: {candidate}")
                df.rename(columns={candidate: 'Tax'}, inplace=True)
                tax_column_found = True
                break

        if not tax_column_found:
            logger.error("The 'Tax' column is missing in the input CSV.")
            return HttpResponse("The 'Tax' column is missing or named differently in the input CSV.", status=400)

        # Check for NaN values in the dataframe before any transformations
        logger.debug(f"Initial NaN count in dataframe:\n{df.isna().sum()}")

        # Clean the data: Fill missing Payee and Description
        df['Payee'] = df['Payee'].fillna('No Name')
        df['Description'] = df['Description'].fillna('.')

        # Log the cleaned data to check for any unintended values like 'x' or 'x.x'
        logger.debug(f"Cleaned dataframe preview:\n{df.head()}")

        # Handle numerical columns
        df['Amount'] = df['Amount'].apply(lambda x: str(x).replace('$', '').replace(',', '').strip())
        df['Tax Amount'] = df['Tax Amount'].apply(lambda x: str(x).replace('$', '').replace(',', '').strip())
        
        # Check for any non-numeric values that may be causing issues
        logger.debug(f"Non-numeric values in Amount column:\n{df[~df['Amount'].apply(lambda x: x.replace('.', '', 1).isdigit())]}")

        # Convert to numeric where appropriate
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
        df['Tax Amount'] = pd.to_numeric(df['Tax Amount'], errors='coerce').fillna(0)

        # Apply tax code mapping
        def map_tax_code(row):
            tax_code = str(row['Tax']).strip()
            return tax_code_mapping.get(tax_code, 'BAS Excluded')

        df['Tax'] = df.apply(map_tax_code, axis=1)

        # Log the transformed data before final output
        logger.debug(f"Transformed dataframe preview:\n{df.head()}")

        # Save the cleaned and transformed dataframe to a CSV
        output_folder = "media/output"
        os.makedirs(output_folder, exist_ok=True)
        allocation_file = os.path.join(output_folder, "converted_allocation.csv")
        df.to_csv(allocation_file, index=False)

        # Return the file as an HTTP response for download
        with open(allocation_file, 'r') as file:
            response = HttpResponse(file.read(), content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="converted_allocation.csv"'

        return response

    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}")
        return HttpResponse(f"Error during conversion: {str(e)}", status=500)
    
@csrf_exempt
def convert_spend_money(request):
    if request.method == 'POST':
        spend_money_file = request.FILES.get('spend_money_file')
        coa_file = request.FILES.get('coa_file')
        job_file = request.FILES.get('job_file')

        if not all([spend_money_file, coa_file, job_file]):
            return JsonResponse({'error': 'All three files are required!'}, status=400)

        # Save uploaded files temporarily
        spend_money_path = default_storage.save('tmp/spend_money_' + spend_money_file.name, spend_money_file)
        coa_path = default_storage.save('tmp/coa_' + coa_file.name, coa_file)
        job_path = default_storage.save('tmp/job_' + job_file.name, job_file)

        spend_money_file_path = os.path.join(settings.MEDIA_ROOT, spend_money_path)
        coa_file_path = os.path.join(settings.MEDIA_ROOT, coa_path)
        job_file_path = os.path.join(settings.MEDIA_ROOT, job_path)

        # Helper to read any file type
        def read_file(file_path):
            ext = file_path.split('.')[-1].lower()
            if ext == 'csv':
                try:
                    return pd.read_csv(file_path, encoding='utf-8')
                except:
                    return pd.read_csv(file_path, encoding='ISO-8859-1')
            elif ext in ['xls', 'xlsx']:
                return pd.read_excel(file_path)
            elif ext == 'txt':
                try:
                    return pd.read_csv(file_path, delimiter='\t', encoding='utf-8')
                except:
                    return pd.read_csv(file_path, delimiter='\t', encoding='ISO-8859-1')
            else:
                raise ValueError("Unsupported file type")

        # Load files
        df = read_file(spend_money_file_path)
        df_coa = read_file(coa_file_path)
        df_jobs = read_file(job_file_path)

        # Column mapping
        column_mapping = {
            "Cheque Account": "Bank",
            "Cheque No.": "Reference",
            "Transaction type": "Transaction type",
            "Date": "Date",
            "Addr 1 - Line 1": "Payee",
            "Memo": "Description",
            "Allocation Account No.": "Account Code",
            "Amount": "Amount",
            "Job No.": "Toption",
            "O": "Tname",
            "Tax Code": "Tax",
            "Tax Amount": "Tax Amount",
            "Currency Code": "Currency Name",
            "Exchange Rate": "Currency rate",
            '': 'Line Amount Type'
        }

        df.rename(columns=column_mapping, inplace=True)
        df.dropna(how='all', inplace=True)
        df['is_group_break'] = df['Bank'].notna()
        df['group_id'] = df['is_group_break'].cumsum()
        df['Reference'] = df['Reference'].astype(str) + '-' + df['group_id'].astype(str)
        df['Bank'] = df['Bank'].ffill()
        df = df[(df['Account Code'] != '') & (df['Account Code'].notna())]
        df['Bank'] = df['Bank'].astype(str).str.replace('-', '', regex=False)
        df['Payee'] = df['Payee'].fillna('No Name')
        df['Transaction type'] = 'SPEND'
        df['Description'] = df['Description'].fillna('.')
        df['Line Amount Type'] = 'Exclusive'

        df_coa.columns = df_coa.columns.str.strip()
        account_codes = df['Account Code'].unique()
        valid_codes = []
        for code in account_codes:
            match = df_coa[df_coa['Account Number'] == code]
            if not match.empty and match.iloc[0]['Account Type'] in ['Bank', 'Credit Card']:
                valid_codes.append(code)

        tax_code_mapping = {
            "GST_Expense": "GST on Expenses",
            "GST_Income": "GST on Income",
            "FRE_Expense": "GST Free Expenses",
            "FRE_Income": "GST Free Income",
            "": "BAS Excluded",
        }

        def map_tracking_option(row):
            match = df_jobs[df_jobs["Job Number"] == row["Toption"]]
            return match["Job Number"].values[0] + "-" + match["Job Name"].values[0] if not match.empty else ""

        df["Toption"] = df.apply(map_tracking_option, axis=1)
        df['Toption'] = df['Toption'].fillna('').astype(str).str.strip()
        df['Tname'] = ''
        df.loc[df['Toption'] != '', 'Tname'] = 'Job'

        df_bank = df[df['Account Code'].isin(valid_codes)].reset_index(drop=True)
        df = df[~df['Account Code'].isin(valid_codes)].reset_index(drop=True)

        column_mapping_bank = {
            "Bank": "From Account",
            "Reference": "Reference Number",
            "Date": "Date",
            "Account Code": "To Account",
            "Amount": "Amount"
        }

        df_bank.rename(columns=column_mapping_bank, inplace=True)
        df_bank['Base Reference'] = df_bank['Reference Number'].str.split('-').str[0]
        df_bank['group_id'] = range(1, len(df_bank) + 1)
        df_bank['Reference Number'] = df_bank['Base Reference'] + '-' + df_bank['group_id'].astype(str)
        df_bank.drop(['Base Reference', 'group_id'], axis=1, inplace=True)

        df["Account Code"] = df["Account Code"].astype(str).str.strip().str.replace('.0', '', regex=False)
        df_coa["Account Number"] = df_coa["Account Number"].astype(str).str.strip()

        def map_tax_code(row):
            tax_code = row["Tax"]
            account_code_str = str(row["Account Code"]).strip()
            coa_row = df_coa[df_coa["Account Number"].astype(str).str.strip() == account_code_str]

            if not coa_row.empty:
                account_type = coa_row["Account Type"].values[0]
                if tax_code == "GST":
                    return tax_code_mapping.get("GST_Income") if account_type in ['Income', 'Other Income'] else tax_code_mapping.get("GST_Expense")
                elif tax_code == "FRE":
                    return tax_code_mapping.get("FRE_Income") if account_type in ['Income', 'Other Income'] else tax_code_mapping.get("FRE_Expense")
            return tax_code_mapping.get(tax_code, "BAS Excluded")

        df["Tax"] = df.apply(map_tax_code, axis=1)

        df_bank["Amount"] = df_bank["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.replace(r"\((.*?)\)", r"-\1", regex=True).astype(float)
        df["Amount"] = df["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.replace(r"\((.*?)\)", r"-\1", regex=True).astype(float)
        df["Tax Amount"] = df["Tax Amount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.replace(r"\((.*?)\)", r"-\1", regex=True).astype(float)

        df = df[[col for col in df.columns if col in column_mapping.values()]]
        df_bank = df_bank[[col for col in df_bank.columns if col in column_mapping_bank.values()]]

        # Output path
        output_dir = os.path.join(settings.MEDIA_ROOT, 'XERO_OUTPUT')
        os.makedirs(output_dir, exist_ok=True)

        spend_output_path = os.path.join(output_dir, 'XERO_SPEND_MONEY.csv')
        bank_output_path = os.path.join(output_dir, 'XERO_BANK_TRANSFER.csv')

        df.to_csv(spend_output_path, index=False)
        df_bank.to_csv(bank_output_path, index=False)

        return JsonResponse({
            'message': 'Conversion successful',
            'spend_money_file': spend_output_path,
            'bank_transfer_file': bank_output_path
        })

    return JsonResponse({'error': 'Invalid request method'}, status=405)