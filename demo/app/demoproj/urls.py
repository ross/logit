from django.urls import path

from pages import views

urlpatterns = [
    path("", views.index, name="index"),
    path("graph.svg", views.graph_svg, name="graph_svg"),
    path("architecture.svg", views.architecture_svg, name="architecture_svg"),
    path("telemetry.js", views.browser_telemetry_js, name="browser_telemetry_js"),
    path("health", views.health, name="health"),
    path("work", views.work, name="work"),
    path("boom", views.boom, name="boom"),
    path("inner", views.inner, name="inner"),
]
