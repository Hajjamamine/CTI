from django.urls import path
from .views import get_predictions

urlpatterns = [
    path('api/predictions/', get_predictions, name='get_predictions'),
]
