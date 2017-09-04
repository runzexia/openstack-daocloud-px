

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
    OK_STATUS_CODE = 200

    def __init__(self, root_helper, driver=None,
                 device_scan_attempts=initiator.DEVICE_SCAN_ATTEMPTS_DEFAULT,
                 *args, **kwargs):
        super(PXConnector, self).__init__(
            root_helper,
            driver=driver,
            device_scan_attempts=device_scan_attempts,
            *args, **kwargs
        )
        self.ATTACH_VOLUME = ["pxctl", "host", "attach"]
        self.DETACH_VOLUME = ["pxctl", "host", "detach"]

    @utils.trace
    @lockutils.synchronized('px', 'px-')
    def connect_volume(self, connection_properties):
        """Connect the volume.

        :param connection_properties: The dictionary that describes all
                                      of the target volume attributes.
        :type connection_properties: dict
        :returns: dict
        """
        LOG.info("Connection Properties : %s",connection_properties)
        self.ATTACH_VOLUME.append(connection_properties['provider_id'])
        try:
            (out, err) = self._execute(*self.ATTACH_VOLUME, run_as_root=True,
                                       root_helper=self._root_helper)
            LOG.info("Map volume %(cmd)s: stdout=%(out)s "
                     "stderr=%(err)s",
                     {'cmd': self.ATTACH_VOLUME, 'out': out, 'err': err})
        except putils.ProcessExecutionError as e:
            msg = (_("Error querying pxctl host attach: %(err)s") % {'err': e.stderr})
            LOG.error(msg)
            raise exception.BrickException(message=msg)
        finally:
            self.ATTACH_VOLUME.pop()
        return '/dev/pxd/pxd'+connection_properties['provider_id']

    def get_search_path(self):
        return "/dev/pxd/pxd"

    def get_volume_paths(self, connection_properties):
        pass

    def extend_volume(self, connection_properties):
        # TODO(walter-boring): is this possible?

        raise NotImplementedError

    @utils.trace
    @lockutils.synchronized('px', 'px-')
    def disconnect_volume(self, connection_properties, device_info):
        """Disconnect the  volume.

        :param connection_properties: The dictionary that describes all
                                      of the target volume attributes.
        :type connection_properties: dict
        :param device_info: historical difference, but same as connection_props
        :type device_info: dict
        """
        LOG.info("Connection Properties : %s",connection_properties)
        self.DETACH_VOLUME.append(connection_properties['provider_id'])
        try:
            (out, err) = self._execute(*self.DETACH_VOLUME, run_as_root=True,
                                       root_helper=self._root_helper)
            LOG.info("DeMap volume %(cmd)s: stdout=%(out)s "
                     "stderr=%(err)s",
                     {'cmd': self.ATTACH_VOLUME, 'out': out, 'err': err})
        except putils.ProcessExecutionError as e:
            msg = (_("Error querying pxctl host detach: %(err)s") % {'err': e.stderr})
            LOG.error(msg)
            raise exception.BrickException(message=msg)
        finally:
            self.DETACH_VOLUME.pop()
        return '/dev/pxd/pxd'+connection_properties['provider_id']