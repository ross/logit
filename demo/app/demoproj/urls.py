from django.urls import path

from pages import views

urlpatterns = [
    path("", views.index, name="index"),
    path("graph.svg", views.graph_svg, name="graph_svg"),
    path("architecture.svg", views.architecture_svg, name="architecture_svg"),
    path("health", views.health, name="health"),
]
