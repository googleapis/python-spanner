"""
instance_id 欠落バグの再現テスト

このスクリプトは、SpannerMetricsTracerFactory が初期化された後、
instance_id が client_attributes に含まれていないことを確認します。
"""

from google.cloud.spanner_v1.metrics.spanner_metrics_tracer_factory import (
    SpannerMetricsTracerFactory,
)
from google.cloud.spanner_v1.metrics.constants import MONITORED_RES_LABEL_KEY_INSTANCE


def test_instance_id_missing():
    """instance_id が設定されていないことを確認するテスト"""

    # シングルトンをリセット（テスト用）
    SpannerMetricsTracerFactory._metrics_tracer_factory = None

    # ファクトリを初期化
    factory = SpannerMetricsTracerFactory(enabled=True)

    print("=== SpannerMetricsTracerFactory の client_attributes ===")
    print(f"client_attributes: {factory.client_attributes}")
    print()

    # instance_id が含まれているかチェック
    has_instance_id = MONITORED_RES_LABEL_KEY_INSTANCE in factory.client_attributes
    print(
        f"instance_id キー ({MONITORED_RES_LABEL_KEY_INSTANCE}): {'✅ 存在' if has_instance_id else '❌ 欠落'}"
    )

    if has_instance_id:
        print(f"  値: {factory.client_attributes[MONITORED_RES_LABEL_KEY_INSTANCE]}")

    print()

    # MetricsTracer を作成して確認
    tracer = factory.create_metrics_tracer()
    if tracer:
        print("=== MetricsTracer の client_attributes ===")
        print(f"client_attributes: {tracer.client_attributes}")

        has_instance_id_in_tracer = (
            MONITORED_RES_LABEL_KEY_INSTANCE in tracer.client_attributes
        )
        print(f"instance_id キー: {'✅ 存在' if has_instance_id_in_tracer else '❌ 欠落'}")
    else:
        print("MetricsTracer: None (OpenTelemetry 未インストール)")

    print()
    print("=== 結論 ===")
    if not has_instance_id:
        print("❌ バグ確認: instance_id が設定されていません")
        print("  -> Cloud Monitoring へのエクスポート時に InvalidArgument エラーが発生します")
    else:
        print("✅ instance_id は正しく設定されています")


if __name__ == "__main__":
    test_instance_id_missing()
