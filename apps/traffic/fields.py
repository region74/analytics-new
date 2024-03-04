from typing import Dict, Any

from apps.datatable.fields import ActionsField


class IPLReportActionsField(ActionsField):
    action_views = {"detail": "api:v1:ipl:detail"}

    def get_action_detail_kwargs(self, record: Any) -> Dict[str, int]:
        return {"pk": record["id"]}
