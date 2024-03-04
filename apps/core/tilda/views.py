from django.http import HttpResponseRedirect
from django.views.generic.base import RedirectView as DjangoRedirectView

from apps.sources.models import TelegramSubscription
from apps.choices import TelegramSubscriptionType


class RedirectView(DjangoRedirectView):
    def get(self, request, *args, **kwargs):
        referrer = request.GET.get("referrer", "")
        destination = request.GET.get("destination", "")
        email = request.GET.get("email", "")
        TelegramSubscription.objects.create(
            action=TelegramSubscriptionType.subscribe.name,
            referrer=referrer,
            destination=destination,
            email=email,
        )
        return HttpResponseRedirect(destination)
