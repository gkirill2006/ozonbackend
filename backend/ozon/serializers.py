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
    class Meta:
        model = OzonSupplyDraft
        fields = [
            "id",
            "logistic_cluster_id",
            "logistic_cluster_name",
            "drop_off_point_warehouse_id",
            "drop_off_point_name",
            "operation_id",
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
            "created_at",
            "updated_at",
        ]


class SupplyBatchStatusSerializer(serializers.ModelSerializer):
    drafts = SupplyDraftSerializer(many=True, read_only=True)

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
