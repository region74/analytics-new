from django.urls import path

from . import views


app_name = "tilda"

urlpatterns = [
    path("redirect/", views.RedirectView.as_view(), name="redirect"),
]
