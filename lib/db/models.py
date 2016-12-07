# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Remove `managed = False` lines for those models you wish to give write DB access
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.
from __future__ import unicode_literals

from django.db import models

class ZeusOmg(models.Model):
    id = models.BigIntegerField(primary_key=True)
    image_url = models.TextField()
    creative_text = models.TextField()
    translated = models.IntegerField()
    class Meta:
        db_table = 'zeus_omg'
