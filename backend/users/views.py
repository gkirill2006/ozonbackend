from django.shortcuts import render, get_object_or_404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics, permissions, status
from django.conf import settings
from .serializers import (
    ConfirmLoginSerializer,
    BotInitSerializer,
    TelegramWebAppLoginSerializer,
    OzonStoreSerializer,
    StoreFilterSettingsSerializer,
)
from django.utils.decorators import method_decorator
from django.utils.crypto import get_random_string
from django.core.cache import cache
from drf_yasg import openapi
from .models import User, OzonStore, StoreFilterSettings
from rest_framework_simplejwt.tokens import RefreshToken
from django.views.decorators.csrf import csrf_exempt



import logging


# Create your views here.
#Создаем пользователя или обновляем и связываем id сессии с аккаунтом       
class SessionLoginAPIView(APIView):
   
    def post(self, request):
        api_key = request.data.get('api_key') or request.headers.get('Api-Key')
        if not api_key or api_key != settings.API_KEY:
            return Response({'error': 'Invalid API key.'}, status=status.HTTP_403_FORBIDDEN)
        
        serializer = ConfirmLoginSerializer(data=request.data)

        # Валидация данных
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Сохраняем данные через сериализатор
            user = serializer.save()
            # Формируем успешный ответ
            return Response({
                'success': True,
                'username': user.username,
                'language_code': user.language_code
            }, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
class BotInitAPIView(APIView):
    def post(self, request):
        api_key = request.data.get('api_key') or request.headers.get('Api-Key')
        if not api_key or api_key != settings.API_KEY:
            return Response({'error': 'Invalid API key.'}, status=status.HTTP_403_FORBIDDEN)

        serializer = BotInitSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.save()
        return Response({
            'success': True,
            'telegram_id': user.telegram_id,
            'username': user.username,
            'language_code': user.language_code
        }, status=status.HTTP_200_OK)
        
class TelegramWebAppLoginAPIView(APIView):
    def post(self, request):
        logger = logging.getLogger(__name__)
        # logger.debug(f"Telegram WebApp initData request: {request.data}")

        serializer = TelegramWebAppLoginSerializer(
            data=request.data,
            context={'telegram_bot_token': settings.TELEGRAM_BOT_TOKEN}
        )

        if not serializer.is_valid():
            logger.warning(f"Invalid Telegram WebApp login attempt: {serializer.errors}")
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.save()
        tokens = serializer.generate_tokens(user)

        return Response({
            'success': True,
            'telegram_id': user.telegram_id,
            'username': user.username,
            'access': tokens['access'],
            'refresh': tokens['refresh']
        }, status=status.HTTP_200_OK)


class GenerateSessionIdView(APIView):
    def get(self, request):
        try:
            session_id = get_random_string(32)
            cache.set(session_id, None, timeout=600)  # session_id valid for 10 minutes
            return Response({'session_id': session_id}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CacheSessionView(APIView):
    def post(self, request):
        session_id = request.data.get('session_id')
        user_id = request.data.get('user_id')
        
        if not session_id or not user_id:
            return Response({'error': 'session_id and user_id are required'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            # Сохраняем session_id с привязкой к пользователю
            cache.set(session_id, user_id, timeout=600)
            return Response({'success': True}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class VerifySessionView(APIView):
    def post(self, request):
        session_id = request.data.get('session_id')
        user_id = request.data.get('user_id')
        
        if not session_id or not user_id:
            return Response({'error': 'session_id and user_id are required'}, status=status.HTTP_400_BAD_REQUEST)
        
        cached_user_id = cache.get(session_id)
        
        if not cached_user_id:
            return Response({'error': 'Invalid or expired session_id'}, status=status.HTTP_400_BAD_REQUEST)
        
        if str(cached_user_id) != str(user_id):
            return Response({'error': 'Session does not belong to this user'}, status=status.HTTP_400_BAD_REQUEST)
        
        return Response({'success': True}, status=status.HTTP_200_OK)


class ObtainTokenView(APIView):


    
    def post(self, request):
        session_id = request.data.get('session_id')

        if not session_id:
            return Response({'error': 'session_id is required'}, status=status.HTTP_400_BAD_REQUEST)

        # Разделяем три состояния: отсутствует ключ / ожидаем / готов
        sentinel = object()
        user_id = cache.get(session_id, sentinel)
        if user_id is sentinel:
            return Response({'error': 'Invalid or expired session_id'}, status=status.HTTP_400_BAD_REQUEST)

        # Если user_id ещё None — сессия создана, но пользователь не подтвердил в боте
        if user_id is None:
            return Response({
                'status': 'pending',
                'message': 'Waiting for user to complete authentication in Telegram bot'
            }, status=status.HTTP_200_OK)

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

        refresh = RefreshToken.for_user(user)
        return Response({
            'status': 'success',
            'refresh': str(refresh),
            'access': str(refresh.access_token),
            'user': {
                'id': user.id,
                'username': user.username,
                'telegram_id': user.telegram_id
            }
        }, status=status.HTTP_200_OK)


@method_decorator(csrf_exempt, name='dispatch')
class DebugLogView(APIView):
    def post(self, request):
        lvl = (request.data.get('level') or 'info').lower()
        message = request.data.get('message') or ''
        meta = request.data.get('meta')
        logger = logging.getLogger('miniapp')
        line = f"[MiniApp] {message} | meta={meta}"
        if lvl in ('error', 'err'):
            logger.error(line)
        elif lvl in ('warn', 'warning'):
            logger.warning(line)
        else:
            logger.info(line)
        return Response({'ok': True})


class UserStoreListCreateView(generics.ListCreateAPIView):
    serializer_class = OzonStoreSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return OzonStore.objects.filter(user=self.request.user).order_by('name', 'client_id')

    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


class UserStoreDetailView(generics.RetrieveUpdateDestroyAPIView):
    serializer_class = OzonStoreSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return OzonStore.objects.filter(user=self.request.user)


class StoreFilterSettingsView(generics.RetrieveUpdateAPIView):
    serializer_class = StoreFilterSettingsSerializer
    permission_classes = [permissions.IsAuthenticated]
    lookup_url_kwarg = 'store_id'

    def get_store(self):
        return get_object_or_404(
            OzonStore,
            pk=self.kwargs[self.lookup_url_kwarg],
            user=self.request.user,
        )

    def get_object(self):
        store = self.get_store()
        obj, _ = StoreFilterSettings.objects.get_or_create(store=store)
        return obj
