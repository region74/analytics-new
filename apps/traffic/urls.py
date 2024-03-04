from django.urls import path

from . import views


app_name = "traffic"

urlpatterns = [
    path("leads/", views.LeadsView.as_view(), name="leads"),
    path("leads/upload/", views.UploadLeadsView.as_view(), name="upload_leads"),
    path("ipl/", views.IPLReportView.as_view(), name="ipl"),
    path("channels/", views.ChannelsView.as_view(), name="channels"),
    path("funnels/", views.FunnelsView.as_view(), name="funnels"),
    path("double/", views.DoubleView.as_view(), name="double"),
    path("telegram/", views.TelegramView.as_view(), name="telegram"),
]
