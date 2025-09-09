from django.urls import path
from .views import (SyncOzonProductView, SyncOzonCategoryTreeView, SyncOzonWarehouseStockView,SyncOzonSalesView,
                    SyncFbsStockView, ProductAnalytics_V2_View, ProductAnalyticsByItemView, TriggerUpdateABCSheetView,
                    CreateOrUpdateAdPlanView)
urlpatterns = [
    path('ozon/products/', SyncOzonProductView.as_view()),
    path('ozon/categories/sync/', SyncOzonCategoryTreeView.as_view()),
    path('ozon/warehouse/sync/', SyncOzonWarehouseStockView.as_view()),
    path('ozon/sales/sync/', SyncOzonSalesView.as_view()),
    path('ozon/fbs-stock/sync/', SyncFbsStockView.as_view()),
    path("ozon/analytics/", ProductAnalytics_V2_View.as_view()),
    
    path("ozon/analytics/products/by-item/", ProductAnalyticsByItemView.as_view()),
    path("ozon/analytics/abc/update", TriggerUpdateABCSheetView.as_view()),
    path("ozon/createorupdateads/", CreateOrUpdateAdPlanView.as_view()),

]