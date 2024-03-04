from .serializers import SendpulseSerializer
from ...views import APIView


class TildaTgApiView(APIView):
    def post(self, request, *args, **kwargs):
        data = self.modify_data(request.data[0])
        serializer = SendpulseSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.get_response()

    def modify_data(self, data):
        modify_data = {
            "action": data.get("title", "subscribe"),
            "referrer": data.get("bot", {}).get("url", ""),
            "destination": "",
        }
        return modify_data
