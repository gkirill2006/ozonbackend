from rest_framework import serializers
from ozon.models import OzonSupplyBatch, OzonSupplyDraft


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
