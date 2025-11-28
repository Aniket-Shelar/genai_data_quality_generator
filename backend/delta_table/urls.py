from django.urls import include, path

from . import views

urlpatterns = [
    path("", views.home_view, name="home_view"),
    path("get_clusters/", views.get_clusters, name="connect"),
    path("run_job/", views.run_job, name="run_job"),
    path("approve_rules/", views.approve_rules, name="approve_rules"),
]

