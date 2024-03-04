from rest_framework import serializers
from apps.sources.models import TelegramSubscription


class SendpulseSerializer(serializers.ModelSerializer):
    class Meta:
        model = TelegramSubscription
        fields = ["action", "referrer", "destination"]
