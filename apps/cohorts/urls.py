from django.urls import path

from . import views


app_name = "cohorts"

urlpatterns = [
    path("zoom/", views.ZoomView.as_view(), name="zoom"),
    path("so/", views.SpecialOffersView.as_view(), name="so"),
    path("expenses/", views.ExpensesView.as_view(), name="expenses"),
    path("offers/", views.TraficOffersView.as_view(), name="offers"),
]
