from django.urls import path

from pages import views

urlpatterns = [
    path("", views.index),
    path("items/", views.items),
    path("items/<int:item_id>/", views.item),
    path("fanout/", views.fanout),
    path("boom/", views.boom),
]
