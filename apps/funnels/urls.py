from django.urls import path, include


app_name = "funnels"


urlpatterns = [
    path("income/", include("apps.funnels.income.urls", namespace="income")),
]
