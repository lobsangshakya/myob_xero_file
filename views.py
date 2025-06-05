import datetime
from datetime import datetime as dt
import io
import json
import os
import re
import tempfile
import traceback
import logging
import csv
from io import StringIO
import pandas as pd
import numpy as np

from django.conf import settings
from django.shortcuts import render, redirect
from django.http import HttpResponse, Http404, JsonResponse
from django.utils.encoding import smart_str
from django.utils.timezone import now
from django.contrib import messages
from django.contrib.auth import authenticate, login as auth_login
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.template.loader import render_to_string

from weasyprint import HTML
from .models import Client


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

            if not input_file.name.lower().endswith('.csv'):
                logger.error(f"Invalid file type uploaded: {input_file.name}")
                return JsonResponse({'error': "Invalid file type. Please upload a CSV file."}, status=400)

            entity_name = request.session.get('entity_name', 'Default')
            safe_entity_name = entity_name.replace(" ", "_")
            date_str = datetime.datetime.now().strftime("%d-%m-%Y")
            filename = f"{safe_entity_name}_Vendor_{date_str}.csv"

            input_file.seek(0)
            try:
                df = pd.read_csv(input_file, encoding='utf-8', skiprows=1)
            except UnicodeDecodeError:
                input_file.seek(0)
                df = pd.read_csv(input_file, encoding='ISO-8859-1', skiprows=1)
            except Exception as e:
                logger.error(f"Failed to read CSV file: {str(e)}")
                return JsonResponse({'error': "Unable to read CSV file. Ensure it is a valid CSV."}, status=400)

            df.columns = df.columns.astype(str).str.strip()

            # Clean curly braces from all string entries
            df = df.applymap(lambda x: str(x).replace("{", "").replace("}", "") if isinstance(x, str) else x)

            column_mapping = {
                "Co./Last Name": "*ContactName",
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
                "Account": "PurchasesAccount"
            }

            # Safely concatenate First Name and Co./Last Name into *ContactName
            if "First Name" in df.columns and "Co./Last Name" in df.columns:
                df["*ContactName"] = df["First Name"].fillna("").astype(str) + " " + df["Co./Last Name"].fillna("").astype(str)
                df["*ContactName"] = df["*ContactName"].str.strip()

            # Safely concatenate BSB and Account Number
            if "BSB" in df.columns and "Account Number" in df.columns:
                df["Account Number"] = df["BSB"].fillna("").astype(str) + df["Account Number"].fillna("").astype(str)
            else:
                logger.warning("BSB or Account Number column missing. Skipping merge operation.")

            df = df.rename(columns=column_mapping)

            # Clean AccountNumber column from asterisks and None strings
            if "AccountNumber" in df.columns:
                df["AccountNumber"] = df["AccountNumber"].astype(str).str.replace(r'\*', '', regex=True)
                df["AccountNumber"] = df["AccountNumber"].replace("None", "").str.strip()

            final_column_order = [
                "*ContactName", "AccountNumber", "EmailAddress", "FirstName", "LastName",
                "POAttentionTo", "POAddressLine1", "POAddressLine2", "POAddressLine3", "POAddressLine4",
                "POCity", "PORegion", "POZipCode", "POCountry", "SAAttentionTo",
                "SAAddressLine1", "SAAddressLine2", "SAAddressLine3", "SAAddressLine4",
                "SACity", "SARegion", "SAZipCode", "SACountry", "PhoneNumber",
                "FaxNumber", "MobileNumber", "DDINumber", "SkypeName",
                "BankAccountName", "BankAccountNumber", "BankAccountParticulars",
                "TaxNumberType", "TaxNumber", "DueDateBillDay", "DueDateBillTerm", "PurchasesAccount"
            ]

            # Remove duplicated columns, then reorder columns if they exist in df
            df = df.loc[:, ~df.columns.duplicated()]
            df = df.reindex(columns=[col for col in final_column_order if col in df.columns])

            output = io.StringIO()
            df.to_csv(output, index=False, encoding='ascii', errors='ignore')
            output.seek(0)

            response = HttpResponse(output.getvalue(), content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            logger.info(f"Vendor CSV conversion successful: {filename}")
            return response

        except Exception as e:
            logger.error(f"Error during Vendor CSV conversion: {str(e)}", exc_info=True)
            return JsonResponse({'error': f"Conversion failed: {str(e)}"}, status=500)

    return JsonResponse({'error': "Invalid request method. Use POST."}, status=405)

        
# Manual Journal Conversion
logger = logging.getLogger(__name__)

# --- Reusable function to read MJ file ---
def read_MJ_file(file_obj, filename):
    ext = filename.split('.')[-1].lower()

    def find_header_row(df_raw):
        for i, row in df_raw.iterrows():
            if row.astype(str).str.contains("ID No", case=False, na=False).any():
                return i
        raise ValueError("Header row containing 'ID No' not found.")

    if ext == "csv":
        try:
            df_raw = pd.read_csv(file_obj, header=None, encoding='utf-8')
        except UnicodeDecodeError:
            file_obj.seek(0)
            df_raw = pd.read_csv(file_obj, header=None, encoding='ISO-8859-1')
        file_obj.seek(0)
        header_row = find_header_row(df_raw)
        df = pd.read_csv(file_obj, header=header_row, encoding='utf-8')
    elif ext in ["xls", "xlsx"]:
        df_raw = pd.read_excel(file_obj, header=None)
        header_row = find_header_row(df_raw)
        file_obj.seek(0)
        df = pd.read_excel(file_obj, header=header_row)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    return df

# --- Fuzzy Column Mapping ---
def map_columns(actual_columns):
    column_mapping = {
        "ID No": ["ID No", "ID No."],
        "Date": ["Date", "Session Date", "Reconciled Date"],
        "Account Code": ["Account Code", "Account No.", "Account No"]
    }

    mapped_columns = {}
    for target, variants in column_mapping.items():
        for variant in variants:
            if variant in actual_columns:
                mapped_columns[target] = variant
                break

    missing = [col for col in column_mapping if col not in mapped_columns]
    return mapped_columns, missing

# --- Main View Function ---
@csrf_exempt
def convert_manual_journal(request):
    logger.debug("convert_manual_journal view called")

    if request.method != 'POST':
        logger.error("Invalid method: Only POST allowed.")
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

        logger.debug(f"Processing file: {input_file.name}, Size: {input_file.size} bytes")
        
        # Read file with header detection
        df = read_MJ_file(input_file, input_file.name)

        logger.debug(f"First 10 rows of {input_file.name}:\n{df.head(10).to_string()}")

        # --- Map Similar Columns ---
        mapped_columns, missing_columns = map_columns(df.columns)
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}. Available columns: {df.columns.tolist()}")
            return JsonResponse({
                'error': f"Missing required columns: {', '.join(missing_columns)}. Available columns: {', '.join(df.columns)}"
            }, status=400)

        # Extract & Rename
        output_df = df[[mapped_columns["ID No"], mapped_columns["Date"], mapped_columns["Account Code"]]].copy()
        output_df.rename(columns={
            mapped_columns["ID No"]: "ID No",
            mapped_columns["Date"]: "Date",
            mapped_columns["Account Code"]: "Account Code"
        }, inplace=True)

        # Prepare CSV response
        output = io.BytesIO()
        output_df.to_csv(output, index=False, encoding="utf-8")
        csv_data = output.getvalue()

        logger.info(f"Manual Journal conversion successful: {filename}")
        response = HttpResponse(csv_data, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['X-Content-Type-Options'] = 'nosniff'
        return response

    except ValueError as ve:
        logger.exception(f"Encoding or header error: {str(ve)}")
        return JsonResponse({'error': str(ve)}, status=400)

    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return JsonResponse({'error': f"Manual Journal conversion failed: {str(e)}"}, status=500)

# Job Conversion
@csrf_exempt
def convert_job(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid request method.'}, status=405)

    try:
        job_file = request.FILES.get("job_file")
        if not job_file:
            return JsonResponse({'error': 'Please upload a Job CSV file.'}, status=400)

        filename = job_file.name
        ext = filename.split('.')[-1].lower()

        def read_file(file_obj, ext):
            if ext == "csv":
                try:
                    df = pd.read_csv(file_obj, encoding='utf-8', header=1)
                except UnicodeDecodeError:
                    file_obj.seek(0)
                    df = pd.read_csv(file_obj, encoding='ISO-8859-1', header=1)
            elif ext in ["xls", "xlsx"]:
                df = pd.read_excel(file_obj, header=1)
            elif ext == "txt":
                try:
                    df = pd.read_csv(file_obj, delimiter='\t', encoding='utf-8', header=1)
                except UnicodeDecodeError:
                    file_obj.seek(0)
                    df = pd.read_csv(file_obj, delimiter='\t', encoding='ISO-8859-1', header=1)
            else:
                raise ValueError("Unsupported file extension.")
            return df

        df = read_file(job_file, ext)

        # Normalize column names
        df.columns = df.columns.str.strip()
        normalized_cols = {col.lower(): col for col in df.columns}
        print("Detected columns:", df.columns.tolist())

        if "job number" in normalized_cols and "job name" in normalized_cols:
            job_number_col = normalized_cols["job number"]
            job_name_col = normalized_cols["job name"]

            df["Job"] = df[job_number_col].astype(str) + "-" + df[job_name_col].astype(str)
            df = df[["Job"]]

            output = io.BytesIO()
            df.to_csv(output, index=False, encoding='utf-8')
            output.seek(0)

            entity_name = request.session.get('entity_name', 'Default')
            out_filename = f"{entity_name.replace(' ', '_')}_Job_{datetime.datetime.now().strftime('%d-%m-%Y')}.csv"

            response = HttpResponse(output, content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{out_filename}"'
            response['X-Content-Type-Options'] = 'nosniff'
            return response
        else:
            return JsonResponse({
                'error': "❌ Required columns 'Job Number' and/or 'Job Name' are missing.",
                'columns_found': df.columns.tolist()
            }, status=400)

    except Exception as e:
        return JsonResponse({'error': f"Job conversion failed: {str(e)}"}, status=500)

def myob_xero_view(request):
    # Simple view to render the template
    return render(request, 'upload.html')

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
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid request method.'}, status=400)

    try:
        item_file = request.FILES.get("item_master_file")
        coa_file = request.FILES.get("coa_file")

        if not item_file or not coa_file:
            return JsonResponse({'error': 'Both Item Master and COA files are required.'}, status=400)

        # Read COA with header on second row (skip top metadata row)
        coa_df = pd.read_csv(coa_file, skiprows=1)
        coa_df.columns = coa_df.columns.str.strip()

        # Read Item Master with header on second row
        item_df = pd.read_csv(item_file, skiprows=1)
        item_df.columns = item_df.columns.str.strip()

        # Check required column
        if "Account Number" not in coa_df.columns:
            return JsonResponse({
                'error': "'Account Number' column missing in COA file.",
                'columns_detected': coa_df.columns.tolist()
            }, status=400)

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

        column_mapping = {
            "Item Number": "*ItemCode",
            "Item Name": "ItemName",
            "Description": "PurchasesDescription",
            "Standard Cost": "PurchasesUnitPrice",
            "Expense/COS Acct": "PurchasesAccount",
            "Tax Code When Bought": "PurchasesTaxRate",
            "Selling Price": "SalesUnitPrice",
            "Income Acct": "SalesAccount",
            "Tax Code When Sold": "SalesTaxRate"
        }

        item_df.rename(columns=column_mapping, inplace=True)

        def map_tax_code(row):
            sales_acc = row.get("SalesAccount")
            tax_code = row.get("SalesTaxRate", "")
            coa_row = coa_df[coa_df["Account Number"] == sales_acc]
            account_type = coa_row.iloc[0]["Account Type"] if not coa_row.empty else ""

            if tax_code == "FRE":
                if account_type in ["Income", "Other Income"]:
                    return tax_code_mapping["FRE_Income"]
                elif account_type in ["Cost of Sales", "Expense", "Other Expense"]:
                    return tax_code_mapping["FRE_Expense"]
                else:
                    return "BAS Excluded"
            elif tax_code == "GST":
                if account_type in ["Income", "Other Income"]:
                    return tax_code_mapping["GST_Income"]
                elif account_type in ["Cost of Sales", "Expense", "Other Expense"]:
                    return tax_code_mapping["GST_Expense"]
                else:
                    return "BAS Excluded"
            else:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

        item_df["SalesTaxRate"] = item_df.apply(map_tax_code, axis=1).fillna("BAS Excluded")

        item_df["PurchasesDescription"] = item_df.get("PurchasesDescription", ".").fillna(".")
        item_df["SalesDescription"] = item_df["PurchasesDescription"]

        final_columns = [
            "*ItemCode", "ItemName", "PurchasesDescription", "PurchasesUnitPrice",
            "PurchasesAccount", "PurchasesTaxRate", "SalesDescription", "SalesUnitPrice",
            "SalesAccount", "SalesTaxRate"
        ]

        for col in final_columns:
            if col not in item_df.columns:
                item_df[col] = ""

        item_df = item_df[final_columns]
        item_df['PurchasesUnitPrice'] = item_df['PurchasesUnitPrice'].replace({r'\$': ''}, regex=True)
        item_df['SalesUnitPrice'] = item_df['SalesUnitPrice'].replace({r'\$': ''}, regex=True)

        entity_name = request.session.get('entity_name', 'Default')
        safe_entity_name = entity_name.replace(" ", "_")
        date_str = datetime.datetime.now().strftime("%d-%m-%Y")
        filename = f"{safe_entity_name}_ItemMaster_{date_str}.csv"

        output = io.BytesIO()
        item_df.to_csv(output, index=False, encoding="utf-8")
        csv_data = output.getvalue()

        response = HttpResponse(csv_data, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['X-Content-Type-Options'] = 'nosniff'

        return response

    except Exception as e:
        return JsonResponse({'error': f'Item Master conversion failed: {str(e)}'}, status=500)
    
@csrf_exempt
def convert_sales_invoice_product(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method."}, status=400)

    # Corrected input names from HTML
    csv_file = request.FILES.get('sales_invoice_product_file')
    coa_file = request.FILES.get('coa_file_product')
    item_file = request.FILES.get('item_file_product')
    job_file = request.FILES.get('job_file_product')

    if not all([csv_file, coa_file, item_file, job_file]):
        return JsonResponse({"error": "One or more required files are missing."}, status=400)

    def read_file(file_obj):
        ext = file_obj.name.split('.')[-1].lower()
        if ext == "csv":
            try:
                return pd.read_csv(file_obj, encoding='utf-8', skiprows=1)
            except UnicodeDecodeError:
                file_obj.seek(0)
                return pd.read_csv(file_obj, encoding='ISO-8859-1', skiprows=1)
        elif ext in ["xls", "xlsx"]:
            return pd.read_excel(file_obj, skiprows=1)
        elif ext == "txt":
            try:
                return pd.read_csv(file_obj, delimiter='\t', encoding='utf-8', skiprows=1)
            except UnicodeDecodeError:
                file_obj.seek(0)
                return pd.read_csv(file_obj, delimiter='\t', encoding='ISO-8859-1', skiprows=1)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    df = read_file(csv_file)
    df_coa = read_file(coa_file)
    df_item = read_file(item_file)
    df_jobs = read_file(job_file)

    # Clean column names
    for d in [df, df_coa, df_item, df_jobs]:
        d.columns = d.columns.str.strip()

    df.dropna(how='all', inplace=True)

    if "First Name" in df.columns and "Co./Last Name" in df.columns:
        df["ContactName"] = df["First Name"].fillna('') + " " + df["Co./Last Name"].fillna('')

    column_mapping = {
        "ContactName": "*ContactName",
        "Invoice No.": "*InvoiceNumber",
        "Date": "*InvoiceDate",
        "Customer PO": "Reference",
        "Item Number": "InventoryItemCode",
        "Quantity": "*Quantity",
        "Description": "*Description",
        "Price": "*UnitAmount",
        "Discount": "Discount",
        "Job": "TrackingOption1",
        "Tax Code": "*TaxType",
        "Tax Amount": "TaxAmount",
        "Currency Code": "Currency",
        "Exchange Rate": "Exchange Rate"
    }
    df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)

    if "*InvoiceDate" in df.columns:
        df["*DueDate"] = df["*InvoiceDate"]

    if "*Description" in df.columns:
        df["*Description"] = df["*Description"].fillna(".")

    account_codes = []
    for item_code in df.get("InventoryItemCode", []):
        match = df_item[df_item["Item Number"] == item_code]
        if not match.empty:
            account_codes.append(match["Income Acct"].iloc[0])
        else:
            account_codes.append(None)
    df["*AccountCode"] = account_codes

    tax_code_map = {
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

    tax_types = []
    df_coa["Account Number"] = df_coa["Account Number"].astype(str)
    for _, row in df.iterrows():
        acct_code = str(int(float(row["*AccountCode"]))) if pd.notna(row["*AccountCode"]) else ""
        tax_code = row.get("*TaxType", "")
        coa_row = df_coa[df_coa["Account Number"] == acct_code]
        if coa_row.empty:
            tax_types.append(tax_code_map.get(tax_code, "BAS Excluded"))
            continue

        acct_type = coa_row.iloc[0]["Account Type"]
        if tax_code == "FRE":
            tax_types.append(tax_code_map.get("FRE_Income" if acct_type == "Income" else "FRE_Expense", "BAS Excluded"))
        elif tax_code == "GST":
            tax_types.append(tax_code_map.get("GST_Income" if acct_type == "Income" else "GST_Expense", "BAS Excluded"))
        else:
            tax_types.append(tax_code_map.get(tax_code, "BAS Excluded"))
    df["*TaxType"] = tax_types

    tracking_list = []
    for val in df.get("TrackingOption1", []):
        match = df_jobs[df_jobs["Job Number"] == val]
        if not match.empty:
            tracking_list.append(f"{match['Job Number'].values[0]}-{match['Job Name'].values[0]}")
        else:
            tracking_list.append(None)
    df["TrackingOption1"] = tracking_list
    df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x else "")

    new_rows = []
    freight_seen = set()
    for _, row in df.iterrows():
        new_rows.append(row)
        freight_amount_raw = row.get("Freight Amount", "0")
        try:
            freight_amount = float(str(freight_amount_raw).replace("$", "").replace(",", "").strip() or "0")
        except:
            freight_amount = 0

        if freight_amount > 0:
            key = (
                row.get("*ContactName", ""),
                row.get("*InvoiceNumber", ""),
                row.get("*InvoiceDate", ""),
                freight_amount,
                row.get("Freight Tax Code", ""),
                row.get("Freight TaxAmount", 0)
            )
            if key not in freight_seen:
                freight_seen.add(key)
                freight_row = row.copy()
                freight_row["InventoryItemCode"] = ""
                freight_row["*AccountCode"] = ""
                freight_row["*Quantity"] = 1
                freight_row["*Description"] = "Freight Charge"
                freight_row["*UnitAmount"] = freight_amount
                freight_row["Discount"] = 0
                freight_row["TrackingName1"] = ""
                freight_row["TrackingOption1"] = ""
                freight_row["*TaxType"] = row.get("Freight Tax Code", "")
                freight_row["TaxAmount"] = row.get("Freight TaxAmount", 0)
                new_rows.append(freight_row)

    df = pd.DataFrame(new_rows)

    # Return as JSON for testing, or export to file here
    return JsonResponse({"status": "success", "row_count": len(df)}, status=200)


def read_file(file_obj, filename):
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.xls', '.xlsx']:
        return pd.read_excel(file_obj)
    else:
        # Try reading CSV with proper header skipping
        df = pd.read_csv(file_obj, header=0).dropna(how='all')
        if df.columns.str.contains('unnamed', case=False).all():
            # If still unnamed, try header=1 (next row)
            file_obj.seek(0)
            df = pd.read_csv(file_obj, header=1).dropna(how='all')
        if df.columns.str.contains('unnamed', case=False).all():
            # Try header=2 as last resort
            file_obj.seek(0)
            df = pd.read_csv(file_obj, header=2).dropna(how='all')
        return df


@csrf_exempt
def convert_sales_invoice_service(request):
    if request.method == 'POST':
        service_file = request.FILES.get('service_invoice_file')
        coa_file = request.FILES.get('coa_file')
        job_file = request.FILES.get('job_file')

        if not service_file or not coa_file or not job_file:
            return HttpResponse("Missing one or more files.", status=400)

        def read_file(file_obj, filename):
            ext = filename.split('.')[-1].lower()
            try:
                if ext == "csv":
                    try:
                        return pd.read_csv(file_obj, encoding='utf-8', skiprows=1)
                    except UnicodeDecodeError:
                        file_obj.seek(0)
                        return pd.read_csv(file_obj, encoding='ISO-8859-1', skiprows=1)
                else:
                    raise ValueError(f"Unsupported file type: {ext}")
            except Exception as e:
                raise ValueError(f"Error reading file {filename}: {e}")

        df = read_file(service_file, service_file.name)
        df_coa = read_file(coa_file, coa_file.name)
        df_jobs = read_file(job_file, job_file.name)

        df = df.dropna(how='all')
        df.columns = df.columns.str.strip()
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

        df = df.rename(columns={col: column_mapping[col] for col in df.columns if col in column_mapping})
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
            account_code = str(int(float(row.get("*AccountCode", 0)))) if pd.notna(row.get("*AccountCode")) else ""
            tax_code = row.get("*TaxType", "")

            df_coa["Account Number"] = df_coa["Account Number"].astype(str)
            coa_row = df_coa[df_coa["Account Number"] == account_code]

            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            account_type = coa_row.iloc[0]["Account Type"]

            if tax_code == "FRE":
                return tax_code_mapping.get("FRE_Income" if account_type == "Income" else "FRE_Expense", tax_code)
            elif tax_code == "GST":
                return tax_code_mapping.get("GST_Income" if account_type == "Income" else "GST_Expense", tax_code)
            else:
                return tax_code_mapping.get(tax_code, tax_code)

        def map_tracking_option(row):
            match = df_jobs[df_jobs["Job Number"] == row["TrackingOption1"]]
            if not match.empty:
                return match.iloc[0]["Job Number"] + "-" + match.iloc[0]["Job Name"]
            return ""

        df["*TaxType"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x != "" else "")
        df["*Quantity"] = 1

        columns_order = [
            "*ContactName", "EmailAddress", "POAddressLine1", "POAddressLine2", "POAddressLine3", "POAddressLine4",
            "POCity", "PORegion", "POPostalCode", "POCountry", "*InvoiceNumber", "Reference", "*InvoiceDate", "*DueDate", "Total",
            "InventoryItemCode", "*Description", "*Quantity", "*UnitAmount", "Discount", "*AccountCode", "*TaxType", "TaxAmount",
            "TrackingName1", "TrackingOption1", "TrackingName2", "TrackingOption2", "Currency", "BrandingTheme", "Exchange Rate"
        ]

        if "*UnitAmount" in df.columns:
            df["*UnitAmount"] = df["*UnitAmount"].replace({r'\$': ''}, regex=True)
        if "TaxAmount" in df.columns:
            df["TaxAmount"] = df["TaxAmount"].replace({r'\$': ''}, regex=True)

        for col in columns_order:
            if col not in df.columns:
                df[col] = ""

        df = df[columns_order]
        df = df.replace({r'\$': ''}, regex=True)

        # Create downloadable CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        response = HttpResponse(output, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=XERO_SERVICE_INVOICE_2.csv'
        return response

    return HttpResponse("Only POST requests are allowed.", status=405)

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
    def read_file(file_obj, ext):
        encodings_to_try = ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']
        if ext in ["xls", "xlsx"]:
            file_obj.seek(0)
            return pd.read_excel(file_obj, header=1)  # Skip fake header row

        for encoding in encodings_to_try:
            try:
                file_obj.seek(0)
                return pd.read_csv(file_obj, encoding=encoding, header=1)  # Skip fake header
            except Exception:
                continue

        file_obj.seek(0)
        content = file_obj.read().decode('utf-8', errors='ignore')
        return pd.read_csv(StringIO(content), header=1)

    if request.method == 'POST' and request.FILES.get('file'):
        try:
            uploaded_file = request.FILES['file']
            filename = uploaded_file.name
            ext = filename.split('.')[-1].lower()

            df = read_file(uploaded_file, ext)
            df.columns = df.columns.astype(str).str.strip()

            print("Uploaded file columns:", df.columns.tolist())

            if df.empty:
                return JsonResponse({"error": "The uploaded file is empty."}, status=400)

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
                "Account Number": "AccountNumber",
                "Account Name": "BankAccountName",
                "A.B.N.": "TaxNumber",
                "Account": "SalesAccount",
                "- Balance Due Days": "DueDateSalesDay",
                "Terms - Payment is Due": "DueDateSalesTerm"
            }

            # Check for required columns
            required_cols = ["First Name", "Co./Last Name", "BSB", "Account Number"]
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                return JsonResponse({"error": f"Missing columns in input file: {missing}"}, status=400)

            length = len(df)

            # Fill and combine names into ContactName
            df["First Name"] = df["First Name"].fillna('')
            df["Co./Last Name"] = df["Co./Last Name"].fillna('')
            df["ContactName"] = (df["First Name"] + " " + df["Co./Last Name"]).str.strip()

            df["BSB"] = df["BSB"].fillna('Unknown')
            df["Account Number"] = df["Account Number"].fillna('Unknown')

            df["AccountNumber"] = df.apply(
                lambda row: f"{row['BSB']}-{row['Account Number']}" if row['BSB'] != 'Unknown' and row['Account Number'] != 'Unknown' else '',
                axis=1
            )

            # Drop original name columns
            df.drop(columns=[col for col in ["First Name", "Co./Last Name"] if col in df.columns], inplace=True)

            # Keep only valid columns
            allowed_cols = ["ContactName"] + [col for col in column_mapping.keys() if col in df.columns]
            df = df.loc[:, [col for col in allowed_cols if col in df.columns]]

            # Rename to Xero format
            df.rename(columns=column_mapping, inplace=True)

            # Clean up AccountNumber if needed
            if "AccountNumber" in df.columns:
                df["AccountNumber"] = df["AccountNumber"].replace(["Unknown", "Unkown", "*None"], "")

            # Final ordering
            cols = ["ContactName"] + [col for col in df.columns if col != "ContactName"]
            df = df[cols]

            # Export to CSV
            output_path = os.path.join(settings.MEDIA_ROOT, "converted_customer.csv")
            df.to_csv(output_path, index=False)

            with open(output_path, 'rb') as f:
                response = HttpResponse(f.read(), content_type='text/csv')
                response['Content-Disposition'] = 'attachment; filename="converted_customer.csv"'
                return response

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Please upload a file using POST with key 'file'."}, status=400)

def main_view(request):
    entity_name = request.session.get('entity_name', '')
    print(f"Main view - Entity name from session: {entity_name}")  # Debugging
    return render(request, 'main.html', {'entity_name': entity_name})

def myob_xero_view(request):
    entity_name = request.session.get('entity_name', '')
    print(f"Myob-xero view - Entity name from session: {entity_name}")  # Debugging
    return render(request, 'myob-xero.html', {'entity_name': entity_name})

def convert_open_ap(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid request method. Use POST.'}, status=405)

    open_ap_file = request.FILES.get('open_ap_file')
    file_encoding = request.POST.get('file_encoding', 'utf-8')

    if not open_ap_file:
        return JsonResponse({'error': 'Open AP file is required.'}, status=400)

    try:
        ext = open_ap_file.name.split('.')[-1].lower()
        if ext not in ['csv', 'xls', 'xlsx']:
            return JsonResponse({'error': 'File must be CSV or Excel (.csv, .xls, .xlsx).'}, status=400)

        # Step 1: Read the file to detect header
        if ext == 'csv':
            encodings = [file_encoding, 'iso-8859-1', 'latin1', 'cp1252', 'utf-16']
            delimiters = [',', ';', '\t', '|']
            temp_df = None
            successful_enc = None
            successful_delim = None

            for enc in encodings:
                for delim in delimiters:
                    try:
                        open_ap_file.seek(0)
                        temp_df = pd.read_csv(open_ap_file, encoding=enc, sep=delim, header=None, nrows=50)
                        # Check if 'ID No.' exists in any column of the first 50 rows
                        if temp_df.apply(lambda row: row.astype(str).str.contains('ID No.', case=False, na=False)).any().any():
                            successful_enc = enc
                            successful_delim = delim
                            break
                    except Exception:
                        continue
                if successful_enc and successful_delim:
                    break

            if not successful_enc or not successful_delim:
                return JsonResponse({'error': 'Unable to parse CSV with tried encodings/delimiters.'}, status=400)

        else:
            open_ap_file.seek(0)
            temp_df = pd.read_excel(open_ap_file, header=None, nrows=50)

        # Step 2: Find header row with 'ID No.'
        header_candidates = temp_df.apply(lambda row: row.astype(str).str.contains('ID No.', case=False, na=False)).any(axis=1)
        if not header_candidates.any():
            return JsonResponse({'error': "Header row with 'ID No.' not found."}, status=400)
        header_row_idx = header_candidates.idxmax()

        # Step 3: Re-read with correct header
        open_ap_file.seek(0)
        if ext == 'csv':
            df = pd.read_csv(open_ap_file, header=header_row_idx, encoding=successful_enc, sep=successful_delim)
        else:
            df = pd.read_excel(open_ap_file, header=header_row_idx)

        # Step 4: Clean column names
        df.columns = df.columns.str.strip().str.replace('\xa0', ' ', regex=False).str.replace(r'\s+', ' ', regex=True)

        # Step 5: Validate required columns
        required_cols = ['ID No.', 'Date', 'Orig. Curr.', 'Total Due']
        available_cols = df.columns.str.lower().str.strip()
        missing_cols = [col for col in required_cols if col.lower() not in available_cols]
        if missing_cols:
            return JsonResponse({
                'error': f'Missing required columns: {", ".join(missing_cols)}. '
                         f'Found columns: {", ".join(df.columns)}'
            }, status=400)

        # Step 6: Column renaming map
        col_map = {}
        for req_col in required_cols:
            for df_col in df.columns:
                if df_col.lower() == req_col.lower():
                    col_map[df_col] = {
                        "ID No.": "*InvoiceNumber",
                        "Date": "*InvoiceDate",
                        "Orig. Curr.": "Currrency",  # Note: Retaining your spelling 'Currrency' as in original code
                        "Total Due": "*UnitAmount"
                    }[req_col]
                    break

        # Step 7: Add *CustomerName column
        current_customer = None
        customer_names = []

        def is_valid_customer_name(name):
            return bool(re.match(r'^[A-Za-z\s,]+$', str(name))) and len(str(name).split()) >= 1

        for idx, row in df.iterrows():
            non_empty_cols = row.notna().sum()
            first_col = row.iloc[0]
            if non_empty_cols <= 2 and pd.notna(first_col) and not str(first_col).startswith(("*", "Total", "Grand", "Ageing", "Payables", "Out of")):
                if isinstance(first_col, str) and is_valid_customer_name(first_col):
                    current_customer = str(first_col).strip()
            customer_names.append(current_customer)

        df['*CustomerName'] = customer_names

        # Step 8: Filter rows with essential data
        df = df[df['Date'].notna() & df['Total Due'].notna() & df['ID No.'].notna()]

        # Step 9: Rename columns
        df.rename(columns=col_map, inplace=True)
        keep_cols = list(col_map.values()) + ['*CustomerName']
        df = df[[col for col in keep_cols if col in df.columns]]

        # Step 10: Add required fixed fields
        df['*DueDate'] = df['*InvoiceDate']
        df['Description'] = "."
        df['*Quantity'] = 1
        df["*TaxType"] = "BAS Excluded"
        df['LineAmountType'] = "Exclusive"
        df["*AccountCode"] = "960"

        # Step 11: Convert date columns to string format (DD/MM/YYYY)
        for date_col in ['*InvoiceDate', '*DueDate']:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%d/%m/%Y')

        # Step 12: Export CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        entity_name = request.POST.get('entity_name', 'unnamed_entity').replace(' ', '_')
        timestamp = datetime.datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
        filename = f'{entity_name}_open_ap_{timestamp}.csv'

        response = HttpResponse(output.getvalue(), content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['X-Content-Type-Options'] = 'nosniff'
        return response

    except Exception as e:
        return JsonResponse({'error': f'Error processing Open AP file: {str(e)}'}, status=500)
    
@csrf_exempt
def convert_open_ar(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    try:
        # Safely get uploaded file
        file_obj = request.FILES.get('open_ar_file')
        if not file_obj:
            return JsonResponse({'error': 'No file uploaded with key "open_ar_file".'}, status=400)

        filename = file_obj.name
        ext = filename.split('.')[-1].lower()

        # Read file based on extension
        if ext == "csv":
            try:
                temp_df = pd.read_csv(file_obj, encoding='utf-8', header=None)
            except UnicodeDecodeError:
                file_obj.seek(0)
                temp_df = pd.read_csv(file_obj, encoding='ISO-8859-1', header=None)
        elif ext in ["xls", "xlsx"]:
            temp_df = pd.read_excel(file_obj, header=None)
        else:
            return JsonResponse({'error': f"Unsupported file extension: {ext}"}, status=400)

        # Find header row containing "ID No."
        header_rows = temp_df[temp_df.apply(lambda row: row.astype(str).str.contains('ID No.', na=False)).any(axis=1)]
        if header_rows.empty:
            return JsonResponse({'error': 'Header row containing "ID No." not found.'}, status=400)

        header_row_idx = header_rows.index[0]

        # Reset file pointer for actual read
        file_obj.seek(0)
        if ext == "csv":
            df = pd.read_csv(file_obj, skiprows=header_row_idx)
        else:
            df = pd.read_excel(file_obj, skiprows=header_row_idx)

        df.columns = df.columns.str.strip()

        # Mapping columns to your format
        column_mapping = {
            "ID No.": "*InvoiceNumber",
            "Date": "*InvoiceDate",
            "Orig. Curr.": "Currency",
            "Total Due": "*UnitAmount"
        }

        current_customer = None
        customer_names = []

        def is_valid_customer_name(name):
            return bool(re.match(r'^[A-Za-z\s]+$', str(name))) and len(str(name).split()) > 1

        for _, row in df.iterrows():
            non_empty_cols = row.notna().sum()
            first_col = row.iloc[0]

            if non_empty_cols == 1 and pd.notna(first_col) and not str(first_col).startswith(("*", "Total", "Grand")):
                if isinstance(first_col, str) and is_valid_customer_name(first_col):
                    current_customer = str(first_col).strip()

            customer_names.append(current_customer)

        df['*CustomerName'] = customer_names

        # Filter rows where Date and Total Due are not empty
        df = df[df['Date'].notna() & df['Total Due'].notna()]
        df.rename(columns=column_mapping, inplace=True)

        # Select and add columns
        df = df[list(column_mapping.values()) + ['*CustomerName']]
        df['*DueDate'] = df['*InvoiceDate']
        df['Description'] = "."
        df['*Quantity'] = 1
        df["*TaxType"] = "BAS Excluded"
        df['LineAmountType'] = "Exclusive"
        df["*AccountCode"] = "960"
        df = df[df['*InvoiceNumber'].notna()]

        final_columns = [
            "*CustomerName", "*InvoiceNumber", "*InvoiceDate", "*DueDate", "*UnitAmount", "Currency",
            "Description", "*Quantity", "*TaxType", "LineAmountType", "*AccountCode"
        ]
        df = df.reindex(columns=final_columns, fill_value="")

        return JsonResponse({
            "success": True,
            "rows": len(df),
            "columns": final_columns,
            "message": "Open AR file processed successfully."
        })

    except Exception as e:
        return JsonResponse({'error': f"Processing error: {str(e)}"}, status=500)

@csrf_exempt
def convert_payroll_journal(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'POST request required'}, status=405)

    payroll_file = request.FILES.get('payroll_journal_file')
    coa_file = request.FILES.get('coa_file')
    job_file = request.FILES.get('job_file')

    if not payroll_file or not coa_file or not job_file:
        return JsonResponse({'error': '❌ Missing one or more required files.'}, status=400)

    try:
        # Read Payroll
        if payroll_file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(payroll_file)
        else:
            df = pd.read_csv(payroll_file)

        # Read COA with fallback header row
        try:
            df_coa = pd.read_csv(coa_file, encoding='ISO-8859-1')
            if not {'Account Type', 'Account Number'}.issubset(df_coa.columns):
                coa_file.seek(0)
                df_coa = pd.read_csv(coa_file, encoding='ISO-8859-1', header=1)
        except Exception:
            coa_file.seek(0)
            df_coa = pd.read_csv(coa_file, encoding='ISO-8859-1', header=1)

        # Read Job file
        df_jobs = pd.read_csv(job_file, encoding='ISO-8859-1')

        # Clean all column headers
        df.columns = df.columns.str.strip().str.replace(r'[^\x00-\x7F]+', '', regex=True)
        df_coa.columns = df_coa.columns.str.strip()
        df_jobs.columns = df_jobs.columns.str.strip()

        # Rename payroll columns
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'id no' in col_lower: col_map[col] = 'Date'
            elif 'account no' in col_lower or 'account code' in col_lower: col_map[col] = 'Account Code'
            elif 'debit' in col_lower: col_map[col] = 'Amount'
            elif 'credit' in col_lower: col_map[col] = 'Credit'
            elif 'tracking' in col_lower: col_map[col] = 'Toption'
        df.rename(columns=col_map, inplace=True)

        # Check payroll required columns
        required = ['Date', 'Account Code', 'Amount', 'Credit']
        missing = [col for col in required if col not in df.columns]
        if missing:
            return JsonResponse({'error': f'Missing columns in payroll journal: {missing}'}, status=400)

        # Rename COA columns
        coa_col_map = {}
        for col in df_coa.columns:
            col_lower = col.lower()
            if 'type' in col_lower: coa_col_map[col] = 'Account Type'
            if 'number' in col_lower or 'code' in col_lower: coa_col_map[col] = 'Account Number'
        df_coa.rename(columns=coa_col_map, inplace=True)

        # Ensure required COA columns exist
        if 'Account Type' not in df_coa.columns or 'Account Number' not in df_coa.columns:
            return JsonResponse({'error': '❌ COA file must contain "Account Type" and "Account Number"'}, status=400)

        # Get bank accounts from COA
        account_type_series = df_coa['Account Type'].fillna('').astype(str).str.lower().str.strip()
        bank_accounts = df_coa.loc[account_type_series == 'bank', 'Account Number'].astype(str).str.strip()

        # Map tracking option
        if 'Toption' in df.columns and 'Job Number' in df_jobs.columns and 'Job Number Xero' in df_jobs.columns:
            df['TrackingOption1'] = df['Toption'].map(
                lambda x: df_jobs[df_jobs['Job Number'] == x]['Job Number Xero'].values[0]
                if not df_jobs[df_jobs['Job Number'] == x].empty else None
            )
            df['TrackingName1'] = 'Job'

        # Add fixed columns
        df['Description'] = 'Payroll'
        df['Transaction type'] = 'SPEND'
        df['Tax'] = 'BAS EXCLUDED'

        # Clean and calculate Amount
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
        df['Credit'] = pd.to_numeric(df.get('Credit', 0), errors='coerce').fillna(0)
        df['Amount'] = df['Amount'] - df['Credit']
        df['Line Amount Type'] = 'Exclusive'

        # Handle Date and Reference
        df['New_Date'] = df['Date'].apply(lambda x: x if not isinstance(x, int) else np.nan)
        df['Reference'] = df['Date'].ffill()
        df['Date'] = df['New_Date'].ffill()
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Reference'] = df['Date'].dt.strftime('%d/%m/%y')
        df['Date'] = df['Date'].dt.strftime('%d/%m/%y')
        df.drop(columns=['New_Date'], inplace=True)

        # Separate Payee from Account Code
        is_numeric_mask = df['Account Code'].astype(str).str.replace('.', '', regex=False).str.isnumeric()
        df['Payee'] = df.loc[~is_numeric_mask, 'Account Code']
        df['Account Code'] = df.loc[is_numeric_mask, 'Account Code']
        df['Payee'] = df['Payee'].ffill()
        df = df[df['Account Code'].notna()]

        # Remove rows with bank account codes
        bank_refs = df[df['Account Code'].astype(str).isin(bank_accounts)].groupby('Reference')['Account Code'].first().rename('Bank Code')
        df = df.merge(bank_refs, on='Reference', how='left')
        df = df[~df['Account Code'].astype(str).isin(bank_accounts)]

        # Make Reference values unique
        df['Ref_Count'] = df.groupby('Reference').cumcount() + 1
        df['Reference'] = df['Reference'] + '-' + df['Ref_Count'].astype(str)
        df.drop(columns=['Ref_Count', 'Credit'], inplace=True)

        # Export to CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        return HttpResponse(output.getvalue(), content_type='text/csv',
                            headers={'Content-Disposition': 'attachment; filename="XERO_PAYROLL.csv"'})

    except Exception as e:
        traceback.print_exc()
        return JsonResponse({'error': f'❌ Processing error: {str(e)}'}, status=500)



    
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

        # Strip all column names
        df.columns = df.columns.str.strip()
        df_coa.columns = df_coa.columns.str.strip()
        df_jobs.columns = df_jobs.columns.str.strip()

        # Add missing columns with empty strings to avoid KeyError
        for col in ['Co./Last Name', 'Purchase No.', 'Date', '- Balance Due Days',
                    'Description', 'Account No.', 'Amount', 'Job', 'Tax Code',
                    'Tax Amount', 'Currency Code', 'Exchange Rate', 'First Name']:
            if col not in df.columns:
                df[col] = ""

        # Rename columns
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
        df.rename(columns={k: v for k, v in column_mapping.items()}, inplace=True)

        # Combine First Name and *ContactName safely
        df["First Name"] = df["First Name"].fillna("")
        df["*ContactName"] = df["First Name"].str.strip() + " " + df["*ContactName"].fillna("").str.strip()
        df["*ContactName"] = df["*ContactName"].str.strip()

        df["*DueDate"] = df["*InvoiceDate"]
        df["*Description"] = df["*Description"].replace("", ".").fillna(".")

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
            account_code = str(row.get("*AccountCode", "")).strip()
            tax_code = row.get("*TaxType", "").strip()

            if not account_code:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            try:
                account_code_int = str(int(float(account_code)))
            except:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            coa_row = df_coa[df_coa["Account Number"].astype(str) == account_code_int]
            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            acc_type = coa_row.iloc[0]["Account Type"]
            if tax_code == "FRE":
                return tax_code_mapping.get("FRE_Income" if acc_type == "Income" else "FRE_Expense", "BAS Excluded")
            elif tax_code == "GST":
                return tax_code_mapping.get("GST_Income" if acc_type == "Income" else "GST_Expense", "BAS Excluded")

            return tax_code_mapping.get(tax_code, "BAS Excluded")

        def map_tracking_option(row):
            tracking_val = row.get("TrackingOption1", "")
            if not tracking_val:
                return ""
            match = df_jobs[df_jobs["Job Number"] == tracking_val]
            return match["Job Number Xero"].values[0] if not match.empty else ""

        df["*TaxType"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x else "")

        columns_order = ["*ContactName", "*InvoiceNumber", "*InvoiceDate", "*DueDate",
                         "*Description", "*AccountCode", "*UnitAmount", "TrackingName1", "TrackingOption1",
                         "*TaxType", "TaxAmount", "*Quantity", "Currency", "Exchange Rate"]

        df_final = df[columns_order]

        output = io.StringIO()
        df_final.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)

        response = HttpResponse(output, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=XERO_PURCHASE_BILL_SERVICE.csv'
        return response
    
@csrf_exempt
def convert_purchase_bill_product(request):
    def read_file(file_obj, filename):
        ext = filename.split('.')[-1].lower()
        skiprows = 1  # skip first row containing '{}'
        if ext == "csv":
            try:
                df = pd.read_csv(file_obj, encoding='utf-8', skiprows=skiprows)
            except UnicodeDecodeError:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, encoding='ISO-8859-1', skiprows=skiprows)
        elif ext in ["xls", "xlsx"]:
            df = pd.read_excel(file_obj, skiprows=skiprows)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
        return df

    def find_column(df, candidates):
        # Return first column found in df from candidates list (case-insensitive)
        cols_lower = {col.lower(): col for col in df.columns}
        for candidate in candidates:
            if candidate.lower() in cols_lower:
                return cols_lower[candidate.lower()]
        return None

    if request.method == 'POST':
        try:
            purchase_file = request.FILES.get('purchase_bill_product_file')
            coa_file = request.FILES.get('coa_file_product')
            item_file = request.FILES.get('item_file_product')
            job_file = request.FILES.get('job_file_product')

            if not all([purchase_file, coa_file, item_file, job_file]):
                return HttpResponse("Missing one or more required files.", status=400)

            # Read files skipping first junk row
            df = read_file(purchase_file, purchase_file.name)
            df_coa = read_file(coa_file, coa_file.name)
            df_item = read_file(item_file, item_file.name)
            df_jobs = read_file(job_file, job_file.name)

            # Strip spaces from columns
            df.columns = df.columns.str.strip()
            df_coa.columns = df_coa.columns.str.strip()
            df_item.columns = df_item.columns.str.strip()
            df_jobs.columns = df_jobs.columns.str.strip()

            df.dropna(how='all', inplace=True)

            # Detect and combine ContactName if parts exist
            first_name_col = find_column(df, ["First Name"])
            last_name_col = find_column(df, ["Co./Last Name", "Last Name", "Surname"])

            if first_name_col and last_name_col:
                df["ContactName"] = df[first_name_col].fillna("") + " " + df[last_name_col].fillna("")

            # Find date column dynamically and rename it to '*InvoiceDate'
            date_col = None
            for col in df.columns:
                if 'date' in col.lower():
                    date_col = col
                    break
            if date_col:
                df.rename(columns={date_col: "*InvoiceDate"}, inplace=True)
            else:
                df["*InvoiceDate"] = pd.NaT

            # Now find and rename other important columns with fallback checks
            mapping_candidates = {
                "*ContactName": ["ContactName", "Contact Name"],
                "*InvoiceNumber": ["Purchase No.", "Invoice No.", "Invoice Number", "Purchase Number"],
                "InventoryItemCode": ["Item Number", "Item No.", "Item Number", "Item"],
                "*Quantity": ["Quantity", "Qty"],
                "*Description": ["Description", "Item Description", "Product Description"],
                "*UnitAmount": ["Price", "Unit Price", "Amount"],
                "TrackingOption1": ["Job", "Job Number", "Tracking Option 1"],
                "*TaxType": ["Tax Code", "Tax Type"],
                "Tax Amount": ["Tax Amount", "TaxAmt"],
                "Currency": ["Currency Code", "Currency"],
                "Exchange Rate": ["Exchange Rate", "Exch Rate"]
            }

            rename_dict = {}
            for new_col, candidates in mapping_candidates.items():
                orig_col = find_column(df, candidates)
                if orig_col:
                    rename_dict[orig_col] = new_col

            df.rename(columns=rename_dict, inplace=True)

            # Convert *InvoiceDate to datetime
            df['*InvoiceDate'] = pd.to_datetime(df['*InvoiceDate'], errors='coerce')

            # *DueDate same as *InvoiceDate
            df["*DueDate"] = df["*InvoiceDate"]
            if "*Description" in df.columns:
                df["*Description"] = df["*Description"].fillna(".")
            else:
                df["*Description"] = "."

            # Prepare tax code mapping dict
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

            # Helper functions
            def map_tax_code(row):
                if "*AccountCode" not in row or "*TaxType" not in row:
                    return "BAS Excluded"
                account_code = str(int(float(row["*AccountCode"]))) if pd.notna(row["*AccountCode"]) else ""
                tax_code = row["*TaxType"] if pd.notna(row["*TaxType"]) else ""
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

            def map_account_code(item_number):
                if pd.isna(item_number):
                    return None
                row = df_item[df_item.apply(lambda x: str(x.get("Item Number", "")).strip() == str(item_number).strip(), axis=1)]
                if not row.empty:
                    inventory_val = row.iloc[0].get("Inventory", None)
                    if pd.notna(inventory_val) and str(inventory_val).strip() != "":
                        return row.iloc[0].get("Asset Acct", None)
                    else:
                        return row.iloc[0].get("Income Acct", None)
                return None

            # Map *AccountCode if InventoryItemCode exists
            if "InventoryItemCode" in df.columns:
                df["*AccountCode"] = df["InventoryItemCode"].apply(map_account_code)
            else:
                df["*AccountCode"] = None

            # Map *TaxType using mapping function
            df["*TaxType"] = df.apply(map_tax_code, axis=1)

            # Map TrackingOption1 using df_jobs
            def map_tracking_option(row):
                val = row.get("TrackingOption1", None)
                if pd.isna(val) or val is None:
                    return None
                match = df_jobs[df_jobs.apply(lambda x: str(x.get("Job Number", "")).strip() == str(val).strip(), axis=1)]
                if not match.empty:
                    return match.iloc[0].get("Job Number Xero", None)
                return None

            if "TrackingOption1" in df.columns:
                df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
                df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if pd.notna(x) and x != "" else "")
            else:
                df["TrackingOption1"] = None
                df["TrackingName1"] = ""

            # Define output columns order, only include columns that exist in df
            columns_order = [
                "*ContactName", "*InvoiceNumber", "*InvoiceDate", "*DueDate",
                "InventoryItemCode", "*AccountCode", "*Quantity", "*Description", "*UnitAmount", "Discount",
                "TrackingName1", "TrackingOption1", "*TaxType", "Tax Amount", "Currency", "Exchange Rate",
            ]
            columns_order = [col for col in columns_order if col in df.columns]

            # Remove $ sign and convert to numeric where needed
            for col in df.columns:
                if df[col].dtype == object and df[col].notna().any():
                    df[col] = df[col].replace({r'\$': ''}, regex=True)

            if "*Quantity" in df.columns:
                df["*Quantity"] = pd.to_numeric(df["*Quantity"], errors="coerce")
                mask = df["*Quantity"] < 0
                df.loc[mask, "*Quantity"] = 4
            else:
                mask = pd.Series([False] * len(df))

            if "*UnitAmount" in df.columns:
                df["*UnitAmount"] = pd.to_numeric(df["*UnitAmount"], errors="coerce")
                if 'mask' in locals():
                    df.loc[mask, "*UnitAmount"] = -df.loc[mask, "*UnitAmount"]

            # Return CSV as response
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="XERO_PURCHASE_BILL_PRODUCT.csv"'
            df.to_csv(path_or_buf=response, columns=columns_order, index=False)

            return response

        except Exception as e:
            return HttpResponse(f"Error processing file: {str(e)}", status=500)

    else:
        return HttpResponse("Only POST method is allowed", status=405)



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

    # ✅ Define tax code mapping (can be reused across views)
    tax_code_mapping = {
        'GST': 'Tax Inclusive',
        'FRE': 'GST Free',
        'BAS': 'BAS Excluded',
        'EXP': 'Export Exempt',
        'NCG': 'Tax Inclusive',  # Non Capital Goods (example)
        'GNR': 'GST Free',       # GST Not Registered
        'INP': 'GST Inclusive',  # Input Taxed Purchase
    }

    try:
        # File reading helper
        def read_file(file, filename, is_main_csv=False):
            ext = filename.split('.')[-1].lower()
            try:
                content = file.read().decode('utf-8-sig', errors='ignore')
                delimiter = ',' if ',' in content.split('\n')[0] else ';'
                df = pd.read_csv(io.StringIO(content), delimiter=delimiter)
                if is_main_csv:
                    logger.debug(f"Initial CSV columns: {df.columns}")
                    df = df.iloc[:, :len(column_mapping)]  # Adjust to expected columns
                    df.columns = list(column_mapping.keys())
                return df
            except Exception as e:
                logger.error(f"Error reading file {filename}: {str(e)}")
                raise

        # Read files
        df = read_file(csv_file, csv_file.name, is_main_csv=True)
        df_coa = read_file(coa_file, coa_file.name)
        df_jobs = read_file(job_file, job_file.name)

        logger.debug(f"Columns in CSV: {df.columns.tolist()}")

        # Handle Payee column variations
        for candidate in ['Payee', 'Co./Last Name', 'Co. / Last Name', 'Last Name']:
            if candidate in df.columns:
                df.rename(columns={candidate: 'Payee'}, inplace=True)
                break
        else:
            return HttpResponse("The 'Payee' column is missing or named differently.", status=400)

        # Handle Description column variations
        for candidate in ['Description', 'Memo', 'Details', 'Comments']:
            if candidate in df.columns:
                df.rename(columns={candidate: 'Description'}, inplace=True)
                break
        else:
            return HttpResponse("The 'Description' column is missing or named differently.", status=400)

        # Handle Tax column variations
        for candidate in ['Tax', 'Tax Code', 'GST Code', 'Tax Type']:
            if candidate in df.columns:
                df.rename(columns={candidate: 'Tax'}, inplace=True)
                break
        else:
            return HttpResponse("The 'Tax' column is missing or named differently.", status=400)

        # Fill missing Payee & Description
        df['Payee'] = df['Payee'].fillna('No Name')
        df['Description'] = df['Description'].fillna('.')

        # Clean Amount fields
        df['Amount'] = df['Amount'].apply(lambda x: str(x).replace('$', '').replace(',', '').strip())
        df['Tax Amount'] = df['Tax Amount'].apply(lambda x: str(x).replace('$', '').replace(',', '').strip())

        # Convert Amount fields to numeric
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
        df['Tax Amount'] = pd.to_numeric(df['Tax Amount'], errors='coerce').fillna(0)

        # Map Tax codes
        def map_tax_code(row):
            tax_code = str(row['Tax']).strip()
            return tax_code_mapping.get(tax_code, 'BAS Excluded')

        df['Tax'] = df.apply(map_tax_code, axis=1)

        # Save to CSV
        output_folder = "media/output"
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, "converted_allocation.csv")
        df.to_csv(output_file, index=False)

        # Return the file for download
        with open(output_file, 'r') as file:
            response = HttpResponse(file.read(), content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="converted_allocation.csv"'
            return response

    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}")
        return HttpResponse(f"Error during conversion: {str(e)}", status=500)
    
@csrf_exempt
def convert_spend_money(request):
    if request.method != 'POST':
        return HttpResponse('Invalid request method.', status=405)

    # Get files using correct keys that match the HTML form 'name' attributes
    spend_money_file = request.FILES.get('spend_money_file')
    coa_file = request.FILES.get('coa_file')      # Correct key for COA file
    job_file = request.FILES.get('job_file')      # Correct key for Job file

    if not all([spend_money_file, coa_file, job_file]):
        return HttpResponse("Missing Spend Money, COA, or Job file.", status=400)

    def read_file(file_obj, filename):
        ext = filename.split('.')[-1].lower()
        if ext == "csv":
            try:
                df = pd.read_csv(file_obj, encoding='utf-8', skiprows=1)
            except UnicodeDecodeError:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, encoding='ISO-8859-1', skiprows=1)
        elif ext in ["xls", "xlsx"]:
            df = pd.read_excel(file_obj, skiprows=1)
        elif ext == "txt":
            try:
                df = pd.read_csv(file_obj, delimiter='\t', encoding='utf-8', skiprows=1)
            except UnicodeDecodeError:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, delimiter='\t', encoding='ISO-8859-1', skiprows=1)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
        return df

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
        "Tax Code": "Tax",
        "Tax Amount": "Tax Amount",
        "Currency Code": "Currency Name",
        "Exchange Rate": "Currency rate",
        '': 'Line Amount Type'
    }

    # Read files into dataframes
    df = read_file(spend_money_file, spend_money_file.name)
    df_coa = read_file(coa_file, coa_file.name)
    df_jobs = read_file(job_file, job_file.name)

    # Rename columns, clean data, and create necessary columns
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
    valid_codes = []
    for code in df['Account Code'].unique():
        match = df_coa[df_coa['Account Number'] == code]
        if not match.empty and match.iloc[0]['Account Type'] in ['Bank', 'Credit Card']:
            valid_codes.append(code)

    def map_tracking_option(row):
        match = df_jobs[df_jobs["Job Number"] == row["Toption"]]
        if not match.empty:
            return f"{match['Job Number'].values[0]}-{match['Job Name'].values[0]}"
        return ""

    df["Toption"] = df.apply(map_tracking_option, axis=1)
    df['Toption'] = df['Toption'].fillna('').astype(str).str.strip()
    df['Tname'] = ''
    df.loc[df['Toption'] != '', 'Tname'] = 'Job'

    df_bank = df[df['Account Code'].isin(valid_codes)].reset_index(drop=True)
    df = df[~df['Account Code'].isin(valid_codes)].reset_index(drop=True)

    df_bank.rename(columns={
        "Bank": "From Account",
        "Reference": "Reference Number",
        "Date": "Date",
        "Account Code": "To Account",
        "Amount": "Amount"
    }, inplace=True)

    df_bank['Base Reference'] = df_bank['Reference Number'].str.split('-').str[0]
    df_bank['group_id'] = range(1, len(df_bank) + 1)
    df_bank['Reference Number'] = df_bank['Base Reference'] + '-' + df_bank['group_id'].astype(str)
    df_bank.drop(['Base Reference', 'group_id'], axis=1, inplace=True)

    tax_code_mapping = {
        "GST_Expense": "GST on Expenses",
        "GST_Income": "GST on Income",
        "FRE_Expense": "GST Free Expenses",
        "FRE_Income": "GST Free Income",
        "": "BAS Excluded",
    }

    def map_tax_code(row):
        tax_code = row["Tax"]
        code = str(row["Account Code"]).strip()
        coa_row = df_coa[df_coa["Account Number"].astype(str).str.strip() == code]
        if not coa_row.empty:
            account_type = coa_row["Account Type"].values[0]
            if tax_code == "GST":
                return tax_code_mapping["GST_Income"] if account_type in ['Income', 'Other Income'] else tax_code_mapping["GST_Expense"]
            elif tax_code == "FRE":
                return tax_code_mapping["FRE_Income"] if account_type in ['Income', 'Other Income'] else tax_code_mapping["FRE_Expense"]
        return tax_code_mapping.get(tax_code, "BAS Excluded")

    df["Tax"] = df.apply(map_tax_code, axis=1)

    df_bank["Amount"] = df_bank["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.replace(r"\((.*?)\)", r"-\1", regex=True).astype(float)
    df["Amount"] = df["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.replace(r"\((.*?)\)", r"-\1", regex=True).astype(float)
    df["Tax Amount"] = df["Tax Amount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.replace(r"\((.*?)\)", r"-\1", regex=True).astype(float)

    columns_order = [
        "Date", "Amount", "Description", "Payee", "Reference", "Transaction type",
        "Account Code", "Tax", "Bank", "ITEM CODE", "Currency rate", "Tname",
        "Toption", "Tname 1", "Toption1", "Line Amount Type", "Tax Amount", "Currency Name"
    ]

    bank_transfer_columns_order = [
        "Date", "Amount", "From Account", "To Account", "Reference Number"
    ]

    df = df.reindex(columns=columns_order, fill_value="")
    df_bank = df_bank.reindex(columns=bank_transfer_columns_order, fill_value="")

    # Prepare zip file response with both CSVs inside
    output_zip_stream = io.BytesIO()
    with zipfile.ZipFile(output_zip_stream, 'w') as zip_file:
        csv1 = io.StringIO()
        df.to_csv(csv1, index=False)
        zip_file.writestr("XERO_SPEND_MONEY.csv", csv1.getvalue())

        csv2 = io.StringIO()
        df_bank.to_csv(csv2, index=False)
        zip_file.writestr("XERO_BANK_TRANSFER_SPEND_MONEY.csv", csv2.getvalue())

    response = HttpResponse(output_zip_stream.getvalue(), content_type='application/zip')
    response['Content-Disposition'] = 'attachment; filename="XERO_SPEND_MONEY_OUTPUT.zip"'
    return response
    
@csrf_exempt
def convert_profsale(request):
    if request.method == 'POST':
        service_file = request.FILES.get('profsale_invoice_file')
        coa_file = request.FILES.get('coa_file')
        job_file = request.FILES.get('job_file')

        if not service_file or not coa_file or not job_file:
            return HttpResponse("Missing one or more files.", status=400)

        def read_file(file_obj, filename):
            ext = filename.split('.')[-1].lower()
            try:
                if ext == "csv":
                    try:
                        return pd.read_csv(file_obj, encoding='utf-8', skiprows=1)
                    except UnicodeDecodeError:
                        file_obj.seek(0)
                        return pd.read_csv(file_obj, encoding='ISO-8859-1', skiprows=1)
                else:
                    raise ValueError(f"Unsupported file type: {ext}")
            except Exception as e:
                raise ValueError(f"Error reading file {filename}: {e}")

        df = read_file(service_file, service_file.name)
        df_coa = read_file(coa_file, coa_file.name)
        df_jobs = read_file(job_file, job_file.name)

        df = df.dropna(how='all')
        df.columns = df.columns.str.strip()
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

        df = df.rename(columns={col: column_mapping[col] for col in df.columns if col in column_mapping})
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
            account_code = str(int(float(row.get("*AccountCode", 0)))) if pd.notna(row.get("*AccountCode")) else ""
            tax_code = row.get("*TaxType", "")

            df_coa["Account Number"] = df_coa["Account Number"].astype(str)
            coa_row = df_coa[df_coa["Account Number"] == account_code]

            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            account_type = coa_row.iloc[0]["Account Type"]

            if tax_code == "FRE":
                return tax_code_mapping.get("FRE_Income" if account_type == "Income" else "FRE_Expense", tax_code)
            elif tax_code == "GST":
                return tax_code_mapping.get("GST_Income" if account_type == "Income" else "GST_Expense", tax_code)
            else:
                return tax_code_mapping.get(tax_code, tax_code)

        def map_tracking_option(row):
            match = df_jobs[df_jobs["Job Number"] == row["TrackingOption1"]]
            if not match.empty:
                return match.iloc[0]["Job Number"] + "-" + match.iloc[0]["Job Name"]
            return ""

        df["*TaxType"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x != "" else "")
        df["*Quantity"] = 1

        columns_order = [
            "*ContactName", "EmailAddress", "POAddressLine1", "POAddressLine2", "POAddressLine3", "POAddressLine4",
            "POCity", "PORegion", "POPostalCode", "POCountry", "*InvoiceNumber", "Reference", "*InvoiceDate", "*DueDate", "Total",
            "InventoryItemCode", "*Description", "*Quantity", "*UnitAmount", "Discount", "*AccountCode", "*TaxType", "TaxAmount",
            "TrackingName1", "TrackingOption1", "TrackingName2", "TrackingOption2", "Currency", "BrandingTheme", "Exchange Rate"
        ]

        if "*UnitAmount" in df.columns:
            df["*UnitAmount"] = df["*UnitAmount"].replace({r'\$': ''}, regex=True)
        if "TaxAmount" in df.columns:
            df["TaxAmount"] = df["TaxAmount"].replace({r'\$': ''}, regex=True)

        for col in columns_order:
            if col not in df.columns:
                df[col] = ""

        df = df[columns_order]
        df = df.replace({r'\$': ''}, regex=True)

        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        response = HttpResponse(output, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=PROFESSIONAL_SALE.csv'
        return response

    return HttpResponse("Only POST requests are allowed.", status=405)

def convert_profpur(request):
    if request.method == 'POST':
        service_file = request.FILES.get('profpur_file')
        coa_file = request.FILES.get('coa_file_profpur')
        job_file = request.FILES.get('job_file_profpur')

        if not all([service_file, coa_file, job_file]):
            return HttpResponse("All three files are required.", status=400)

        df = read_file(service_file, service_file.name)
        df_coa = read_file(coa_file, coa_file.name)
        df_jobs = read_file(job_file, job_file.name)

        df.dropna(how='all', inplace=True)

        # Strip all column names
        df.columns = df.columns.str.strip()
        df_coa.columns = df_coa.columns.str.strip()
        df_jobs.columns = df_jobs.columns.str.strip()

        # Add missing columns with empty strings to avoid KeyError
        for col in ['Co./Last Name', 'Purchase No.', 'Date', '- Balance Due Days',
                    'Description', 'Account No.', 'Amount', 'Job', 'Tax Code',
                    'Tax Amount', 'Currency Code', 'Exchange Rate', 'First Name']:
            if col not in df.columns:
                df[col] = ""

        # Rename columns
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
        df.rename(columns={k: v for k, v in column_mapping.items()}, inplace=True)

        # Combine First Name and *ContactName safely
        df["First Name"] = df["First Name"].fillna("")
        df["*ContactName"] = df["First Name"].str.strip() + " " + df["*ContactName"].fillna("").str.strip()
        df["*ContactName"] = df["*ContactName"].str.strip()

        df["*DueDate"] = df["*InvoiceDate"]
        df["*Description"] = df["*Description"].replace("", ".").fillna(".")

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
            account_code = str(row.get("*AccountCode", "")).strip()
            tax_code = row.get("*TaxType", "").strip()

            if not account_code:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            try:
                account_code_int = str(int(float(account_code)))
            except:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            coa_row = df_coa[df_coa["Account Number"].astype(str) == account_code_int]
            if coa_row.empty:
                return tax_code_mapping.get(tax_code, "BAS Excluded")

            acc_type = coa_row.iloc[0]["Account Type"]
            if tax_code == "FRE":
                return tax_code_mapping.get("FRE_Income" if acc_type == "Income" else "FRE_Expense", "BAS Excluded")
            elif tax_code == "GST":
                return tax_code_mapping.get("GST_Income" if acc_type == "Income" else "GST_Expense", "BAS Excluded")

            return tax_code_mapping.get(tax_code, "BAS Excluded")

        def map_tracking_option(row):
            tracking_val = row.get("TrackingOption1", "")
            if not tracking_val:
                return ""
            match = df_jobs[df_jobs["Job Number"] == tracking_val]
            return match["Job Number Xero"].values[0] if not match.empty else ""

        df["*TaxType"] = df.apply(map_tax_code, axis=1).fillna("BAS Excluded")
        df["TrackingOption1"] = df.apply(map_tracking_option, axis=1)
        df["TrackingName1"] = df["TrackingOption1"].apply(lambda x: "Job" if x else "")

        columns_order = ["*ContactName", "*InvoiceNumber", "*InvoiceDate", "*DueDate",
                         "*Description", "*AccountCode", "*UnitAmount", "TrackingName1", "TrackingOption1",
                         "*TaxType", "TaxAmount", "*Quantity", "Currency", "Exchange Rate"]

        df_final = df[columns_order]

        output = io.StringIO()
        df_final.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)

        response = HttpResponse(output, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=PROFPUR.csv'  # filename changed here
        return response
    
@csrf_exempt
def convert_duplicate_bill_service(request):
    if request.method != 'POST':
        return HttpResponse(json.dumps({'error': 'Invalid request method'}),
                            content_type='application/json', status=405)

    try:
        bill_file = request.FILES.get('duplicate_bill_service_file')
        coa_file = request.FILES.get('coa_file_duplicate')

        if not bill_file or not coa_file:
            return HttpResponse(json.dumps({'error': 'Both Duplicate Bill Service and COA files are required'}),
                                content_type='application/json', status=400)

        def read_file(file_obj, filename):
            ext = filename.split('.')[-1].lower()

            # Peek into first few bytes to detect if first line is {}
            peek = file_obj.read(512).decode('utf-8', errors='ignore')
            file_obj.seek(0)
            skip_row = 1 if peek.strip().startswith('{') else 0

            if ext == "csv":
                df = pd.read_csv(file_obj, encoding='utf-8', skiprows=skip_row)
            elif ext in ["xls", "xlsx"]:
                xls = pd.ExcelFile(file_obj)
                dfs = [pd.read_excel(xls, sheet_name=sheet, skiprows=skip_row) for sheet in xls.sheet_names]
                df = pd.concat(dfs, ignore_index=True)
            elif ext == "txt":
                df = pd.read_csv(file_obj, delimiter='\t', encoding='utf-8', skiprows=skip_row)
            else:
                raise ValueError(f"Unsupported file extension: {ext}")
            
            df.columns = df.columns.str.strip()
            return df

        df = read_file(bill_file, bill_file.name)
        df_coa = read_file(coa_file, coa_file.name)

        columns_mapping = {
            'C-Name': '*ContactName',
            'ID': '*InvoiceNumber',
            'date': '*InvoiceDate',
            'Quantity': '*Quantity',
            'LineTotalamt': '*UnitAmount',
            'AccountCode': '*AccountCode',
            'TaxCode': '*TaxType',
            'Job': 'TrackingName1',
            'IsTaxInclusive': 'LineAmountTypes'
        }
        df.rename(columns=columns_mapping, inplace=True)

        df['*InvoiceDate'] = df['*InvoiceDate'].astype(str).str.split(':').str[1]
        df['*DueDate'] = df['*InvoiceDate']

        df['Description'] = df.get('Description', pd.Series(['.'] * len(df)))
        df['*Quantity'] = df['*Quantity'].fillna('1')

        df['*AccountCode'] = df['*AccountCode'].astype(str).replace(r'ACC:|\-', '', regex=True)

        tax_code_mapping = {
            "CAP": "GST on Capital",
            "GST_Expense": "GST on Expenses",
            "GST_Income": "GST on Income",
            "FRE_Expense": "GST Free Expenses",
            "FRE_Income": "GST Free Income",
            "IMP": "GST on Imports",
            "INP": "Input Taxed"
        }

        def map_tax_codes(row):
            tax_code = row['*TaxType']
            account_code = row['*AccountCode']
            match = df_coa[df_coa['Account Number'].astype(str).str.strip() == str(account_code).strip()]
            if not match.empty:
                acc_type = match['Account Type'].values[0]
                if tax_code == 'GST':
                    return tax_code_mapping.get(f'GST_{acc_type}', 'BAS Excluded')
                elif tax_code == 'FRE':
                    return tax_code_mapping.get(f'FRE_{acc_type}', 'BAS Excluded')
            return tax_code_mapping.get(tax_code, 'BAS Excluded')

        df['*TaxType'] = df.apply(map_tax_codes, axis=1)

        df['TrackingOption1'] = np.where(
            df.get('Job-no', '').notna() & df.get('Job-Name', '').notna(),
            df['Job-no'].astype(str).str.strip() + '-' + df['Job-Name'].astype(str).str.strip(),
            ''
        )
        df['TrackingName1'] = df['TrackingOption1'].apply(lambda x: 'Job' if str(x).strip() != '' else '')

        df['LineAmountTypes'] = df['LineAmountTypes'].apply(lambda x: 'Inclusive' if str(x).lower() == 'true' else 'Exclusive')

        df['*InvoiceNumber'] = (
            df['Number'].astype(str).str.strip() + '-' +
            df['*InvoiceNumber'].astype(str).str.strip().str.split('-').str[0]
        )

        output_columns = [
            "*ContactName", "EmailAddress", "POAddressLine1", "POAddressLine2", "POAddressLine3",
            "POAddressLine4", "POCity", "PORegion", "POPostalCode", "POCountry",
            "*InvoiceNumber", "*InvoiceDate", "*DueDate", "Total", "InventoryItemCode",
            "Description", "*Quantity", "*UnitAmount", "*AccountCode", "*TaxType",
            "TaxAmount", "TrackingName1", "TrackingOption1", "TrackingName2", "TrackingOption2",
            "Currency", "LineAmountTypes"
        ]

        for col in output_columns:
            if col not in df.columns:
                df[col] = ''

        df = df[output_columns]

        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)

        return HttpResponse(
            output.getvalue(),
            content_type='text/csv',
            headers={'Content-Disposition': 'attachment; filename="XERO_DUPLICATE_BILL_SERVICE.csv"'}
        )

    except Exception as e:
        return HttpResponse(json.dumps({'error': str(e)}),
                            content_type='application/json', status=500)

