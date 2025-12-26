from rest_framework import serializers
from ozon.models import (
    OzonSupplyBatch,
    OzonSupplyDraft,
    OzonFbsPosting,
    OzonFbsPostingLabel,
    OzonBotSettings,
)


class DraftItemSerializer(serializers.Serializer):
    sku = serializers.IntegerField()
    quantity = serializers.IntegerField(min_value=0)


class DraftShipmentSerializer(serializers.Serializer):
    warehouse = serializers.CharField(max_length=255)
    items = DraftItemSerializer(many=True)


class DraftDestinationSerializer(serializers.Serializer):
    warehouse_id = serializers.IntegerField()
    name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    address = serializers.CharField(max_length=500, required=False, allow_blank=True)


class DraftCreateSerializer(serializers.Serializer):
    store_id = serializers.IntegerField()
    supplyType = serializers.CharField(max_length=64)
    destinationWarehouse = DraftDestinationSerializer()
    shipments = DraftShipmentSerializer(many=True)

    def validate_shipments(self, shipments):
        if not shipments:
            raise serializers.ValidationError("Не переданы склады для поставки.")
        return shipments


class SupplyDraftSerializer(serializers.ModelSerializer):
    supply_order_states = serializers.SerializerMethodField()

    class Meta:
        model = OzonSupplyDraft
        fields = [
            "id",
            "logistic_cluster_id",
            "logistic_cluster_name",
            "drop_off_point_warehouse_id",
            "drop_off_point_name",
            "operation_id",
            "operation_id_supply",
            "draft_id",
            "supply_warehouse",
            "selected_supply_warehouse",
            "timeslot_response",
            "selected_timeslot",
            "timeslot_updated_at",
            "status",
            "attempts",
            "next_attempt_at",
            "error_message",
            "supply_order_ids",
            "supply_order_response",
            "supply_bundle_items",
            "supply_order_states",
            "supply_status_updated_at",
            "created_at",
            "updated_at",
        ]

    def get_supply_order_states(self, obj):
        """
        Возвращает список статусов заявок на поставку, если они есть в сохраненном ответе.
        """
        orders = (obj.supply_order_response or {}).get("orders") or []
        return [o.get("state") for o in orders if isinstance(o, dict) and o.get("state")]


class SupplyBatchStatusSerializer(serializers.ModelSerializer):
    drafts = serializers.SerializerMethodField()

    class Meta:
        model = OzonSupplyBatch
        fields = [
            "batch_id",
            "batch_seq",
            "store",
            "status",
            "drop_off_point_warehouse_id",
            "drop_off_point_name",
            "created_at",
            "updated_at",
            "drafts",
        ]

    def get_drafts(self, obj):
        qs = obj.drafts.exclude(status="created")
        return SupplyDraftSerializer(qs, many=True).data


class SupplyBatchConfirmedSerializer(serializers.ModelSerializer):
    drafts = serializers.SerializerMethodField()

    class Meta:
        model = OzonSupplyBatch
        fields = [
            "batch_id",
            "batch_seq",
            "store",
            "status",
            "drop_off_point_warehouse_id",
            "drop_off_point_name",
            "created_at",
            "updated_at",
            "drafts",
        ]

    def get_drafts(self, obj):
        qs = obj.drafts.filter(status="created")
        return SupplyDraftSerializer(qs, many=True).data


class FbsPostingSyncSerializer(serializers.Serializer):
    store_id = serializers.IntegerField()
    status = serializers.CharField(required=False, allow_blank=True)
    since = serializers.DateTimeField(required=False, allow_null=True)
    to = serializers.DateTimeField(required=False, allow_null=True)
    limit = serializers.IntegerField(required=False, min_value=1, max_value=1000, default=1000)
    return_data = serializers.BooleanField(required=False, default=True)


class FbsPostingSerializer(serializers.ModelSerializer):
    label_ready = serializers.SerializerMethodField()
    label_status = serializers.SerializerMethodField()
    label_file_url = serializers.SerializerMethodField()
    label_file_path = serializers.SerializerMethodField()

    class Meta:
        model = OzonFbsPosting
        fields = [
            "id",
            "posting_number",
            "order_id",
            "order_number",
            "status",
            "substatus",
            "delivery_method_id",
            "delivery_method_name",
            "delivery_method_warehouse_id",
            "delivery_method_warehouse",
            "tpl_provider_id",
            "tpl_provider",
            "tpl_integration_type",
            "tracking_number",
            "in_process_at",
            "shipment_date",
            "delivering_date",
            "status_changed_at",
            "awaiting_packaging_at",
            "awaiting_deliver_at",
            "acceptance_in_progress_at",
            "delivering_at",
            "delivered_at",
            "cancelled_at",
            "archived_at",
            "needs_label",
            "labels_printed_at",
            "print_count",
            "label_ready",
            "label_status",
            "label_file_url",
            "label_file_path",
            "products",
            "available_actions",
            "cancellation",
            "last_seen_at",
            "last_synced_at",
            "created_at",
            "updated_at",
        ]

    def _get_label(self, obj):
        label_type = self.context.get("label_type") or OzonFbsPostingLabel.TASK_TYPE_BIG
        prefetched = getattr(obj, "prefetched_labels", None)
        if prefetched is not None:
            return prefetched[0] if prefetched else None
        return (
            obj.labels.filter(task_type=label_type)
            .order_by("-updated_at")
            .first()
        )

    def get_label_ready(self, obj):
        label = self._get_label(obj)
        return bool(label and label.status == "completed" and label.file_path)

    def get_label_status(self, obj):
        label = self._get_label(obj)
        return label.status if label else ""

    def get_label_file_url(self, obj):
        label = self._get_label(obj)
        return label.file_url if label else ""

    def get_label_file_path(self, obj):
        label = self._get_label(obj)
        return label.file_path if label else ""


class FbsPostingLiteSerializer(serializers.ModelSerializer):
    products = serializers.SerializerMethodField()

    class Meta:
        model = OzonFbsPosting
        fields = [
            "id",
            "posting_number",
            "order_id",
            "order_number",
            "status",
            "delivery_method_name",
            "delivery_method_warehouse",
            "tracking_number",
            "in_process_at",
            "shipment_date",
            "delivering_date",
            "status_changed_at",
            "delivering_at",
            "labels_printed_at",
            "print_count",
            "products",
            "last_seen_at",
            "last_synced_at",
            "created_at",
            "updated_at",
        ]

    def get_products(self, obj):
        items = obj.products or []
        if not isinstance(items, list):
            return []
        allowed_keys = {"sku", "imei", "name", "price", "offer_id", "quantity"}
        result = []
        for item in items:
            if isinstance(item, dict):
                filtered = {key: item.get(key) for key in allowed_keys if key in item}
                result.append(filtered)
        return result


class FbsPostingPrintSerializer(serializers.Serializer):
    store_id = serializers.IntegerField()
    posting_numbers = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True,
    )
    posting_ids = serializers.ListField(
        child=serializers.IntegerField(),
        required=False,
        allow_empty=True,
    )
    force = serializers.BooleanField(required=False, default=False)

    def validate(self, attrs):
        numbers = attrs.get("posting_numbers") or []
        ids = attrs.get("posting_ids") or []
        if not numbers and not ids:
            raise serializers.ValidationError("posting_numbers or posting_ids is required")
        return attrs


class FbsPostingLabelsSerializer(serializers.Serializer):
    store_id = serializers.IntegerField()
    posting_numbers = serializers.ListField(child=serializers.CharField(), allow_empty=False)
    label_type = serializers.ChoiceField(
        choices=[OzonFbsPostingLabel.TASK_TYPE_BIG, OzonFbsPostingLabel.TASK_TYPE_SMALL],
        required=False,
        default=OzonFbsPostingLabel.TASK_TYPE_BIG,
    )
    wait_seconds = serializers.IntegerField(required=False, min_value=0, max_value=30, default=2)


class BotSettingsSerializer(serializers.ModelSerializer):
    class Meta:
        model = OzonBotSettings
        fields = [
            "pdf_sort_mode",
            "pdf_sort_ascending",
            "updated_at",
        ]
        read_only_fields = ["updated_at"]


class FbsPostingRefreshSerializer(serializers.Serializer):
    store_id = serializers.IntegerField()
    status = serializers.CharField(required=False, allow_blank=True)
    since = serializers.DateTimeField(required=False, allow_null=True)
    to = serializers.DateTimeField(required=False, allow_null=True)
    limit = serializers.IntegerField(required=False, min_value=1, max_value=1000, default=1000)
