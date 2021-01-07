from django.db import models

# Create your models here.
class Avails(models.Model):
    svod_avails = models.FileField()
    ptv_avails = models.FileField()
    ptv_local_avails = models.FileField()
    screeners = models.FileField()
    ratings = models.FileField()
