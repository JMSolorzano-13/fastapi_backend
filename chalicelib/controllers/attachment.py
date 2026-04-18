from chalicelib.controllers.common import CommonController
from chalicelib.schema.models.tenant.attachment import Attachment


class AttachmentController(CommonController):
    model = Attachment

    default_read_fields = set()
