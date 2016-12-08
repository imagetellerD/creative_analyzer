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

class ZeusCreative(models.Model):
    id = models.BigIntegerField(primary_key=True)
    name = models.CharField(max_length=128)
    fb_creative_id = models.BigIntegerField()
    zeus_user_id = models.BigIntegerField()
    fb_user_id = models.BigIntegerField()
    fb_account_id = models.BigIntegerField()
    image_id = models.BigIntegerField()
    video_id = models.BigIntegerField()
    button_type = models.IntegerField()
    create_time = models.IntegerField()
    update_time = models.IntegerField()
    status = models.IntegerField()
    text_id = models.BigIntegerField()
    zeus_promotion_id = models.BigIntegerField()
    image_ids = models.TextField()
    text_ids = models.TextField()
    video_ids = models.TextField()
    class Meta:
        db_table = 'zeus_creative'

class ZeusCreativeTextLib(models.Model):
    id = models.BigIntegerField(primary_key=True)
    zeus_user_id = models.BigIntegerField()
    fb_user_id = models.BigIntegerField()
    fb_account_id = models.BigIntegerField()
    text = models.TextField()
    shared = models.IntegerField()
    create_time = models.BigIntegerField()
    update_time = models.BigIntegerField()
    status = models.IntegerField()
    name = models.CharField(max_length=64)
    text_hash = models.BigIntegerField()
    language = models.CharField(max_length=32)
    platform = models.TextField()
    source_language_id = models.BigIntegerField()
    group_id = models.BigIntegerField()
    class Meta:
        db_table = 'zeus_creative_text_lib'

class ZeusImageLib(models.Model):
    id = models.BigIntegerField(primary_key=True)
    fb_image_id = models.CharField(max_length=32)
    fb_image_id_hash = models.BigIntegerField()
    fb_account_id = models.BigIntegerField()
    image_url = models.CharField(max_length=255)
    origin_width = models.IntegerField()
    origin_height = models.IntegerField()
    create_time = models.BigIntegerField()
    marked = models.IntegerField()
    image_ratio_type = models.IntegerField()
    class Meta:
        db_table = 'zeus_image_lib'

class ZeusOmg(models.Model):
    id = models.BigIntegerField(primary_key=True)
    creative_id = models.BigIntegerField()
    image_url = models.TextField()
    creative_text = models.TextField()
    translated = models.IntegerField()
    class Meta:
        db_table = 'zeus_omg'

