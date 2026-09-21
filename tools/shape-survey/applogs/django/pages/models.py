from django.db import models


class Item(models.Model):
    """The one model -- enough to make `/items/` a real database query and therefore a real
    `opentelemetry-instrumentation-dbapi` client span."""

    name = models.CharField(max_length=64)
    price = models.IntegerField(default=0)
    created = models.DateTimeField(auto_now_add=True)
