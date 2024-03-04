from django.urls import path

from . import views


app_name = "sendpulse"

urlpatterns = [
    path("subscription/", views.TildaTgApiView.as_view(), name="subscription"),
]
