from django.db import models
from django.contrib.auth.models import User

class Client(models.Model):
    name = models.CharField(max_length=255)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    # Optional: entity_name = models.CharField(max_length=255)

    def __str__(self):
        return self.name
