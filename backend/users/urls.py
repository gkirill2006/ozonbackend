from django.urls import path
from .views import (
    SessionLoginAPIView,
    # BotInitAPIView,
    # TelegramWebAppLoginAPIView,
    GenerateSessionIdView,
    ObtainTokenView,
    # CacheSessionView,
    # VerifySessionView,
    DebugLogView,
    UserStoreListCreateView,
    UserStoreDetailView,
    StoreFilterSettingsView,
    StoreInviteView,
    StoreInviteRespondView,
    StoreInviteListView,
    StoreAccessManageView,
)

urlpatterns = [
    path('session-login/', SessionLoginAPIView.as_view(), name='session-login'),
    # path('bot-init/', BotInitAPIView.as_view(), name='bot-init'),
    # path('telegram-webapp-login/', TelegramWebAppLoginAPIView.as_view(), name='telegram-webapp-login'),
    path('generate-session-id/', GenerateSessionIdView.as_view(), name='generate-session-id'),
    path('obtain-token/', ObtainTokenView.as_view(), name='obtain-token'),
    # path('cache-session/', CacheSessionView.as_view(), name='cache-session'),
    # path('verify-session/', VerifySessionView.as_view(), name='verify-session'),
    path('miniapp-log/', DebugLogView.as_view(), name='miniapp-log'),
    path('4bBFJCoiYnhFjbz3awRJ5LorPYLVtUNy/', SessionLoginAPIView.as_view(), name='session-login'),
    path('stores/', UserStoreListCreateView.as_view(), name='user-store-list'),
    path('stores/<int:pk>/', UserStoreDetailView.as_view(), name='user-store-detail'),
    path('stores/<int:store_id>/filters/', StoreFilterSettingsView.as_view(), name='store-filter-settings'),
    path('stores/<int:store_id>/invite/', StoreInviteView.as_view(), name='store-invite'),
    path('stores/<int:store_id>/invite/respond/', StoreInviteRespondView.as_view(), name='store-invite-respond'),
    path('stores/invites/', StoreInviteListView.as_view(), name='store-invite-list'),
    path('stores/<int:store_id>/accesses/', StoreAccessManageView.as_view(), name='store-access-list'),
    path('stores/<int:store_id>/accesses/<uuid:user_id>/', StoreAccessManageView.as_view(), name='store-access-delete'),

]
