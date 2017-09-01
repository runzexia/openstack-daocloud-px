import json
import os
import requests
from six.moves import urllib

from oslo_concurrency import lockutils
from oslo_concurrency import processutils as putils
from oslo_log import log as logging

from os_brick import exception
from os_brick.i18n import _
from os_brick import initiator
from os_brick.initiator.connectors import base
from os_brick import utils

LOG = logging.getLogger(__name__)
DEVICE_SCAN_ATTEMPTS_DEFAULT = 3
synchronized = lockutils.synchronized_with_prefix('os-brick-')


class PXConnector(base.BaseLinuxConnector):