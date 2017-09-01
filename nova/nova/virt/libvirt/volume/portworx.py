

"""Libvirt volume driver for PX."""
from oslo_log import log as logging

import nova.conf

from nova.virt.libvirt.volume import volume as libvirt_volume

LOG = logging.getLogger(__name__)

CONF = nova.conf.CONF


class LibvirtPXVolumeDriver(libvirt_volume.LibvirtBaseVolumeDriver):
    """Class PX Libvirt volume Driver

    Implements Libvirt part of volume driver for PX cinder driver.
    Uses the PX connector from the os-brick projects
    """
    def __init__(self, host):
        super(LibvirtPXVolumeDriver, self).__init__(host,
                                                        is_block_dev=False)

    def get_config(self, connection_info, disk_info):
        conf = super(LibvirtPXVolumeDriver, self).get_config(
            connection_info, disk_info)
        LOG.warning("get_config")
        LOG.warning(connection_info)
        conf.source_type = 'block'
        conf.source_path = connection_info['data']['device_path']
        return conf

    def connect_volume(self, connection_info, disk_info):
        LOG.warning("connect_volume_step1")
        LOG.warning(connection_info)
        LOG.warning(disk_info)
        disk_info['path'] = "/dev/pxd/pxd576325260945142478"
        connection_info['data']['device_path'] = disk_info['path']

    def disconnect_volume(self, connection_info, disk_dev):

        super(LibvirtPXVolumeDriver, self).disconnect_volume(
            connection_info, disk_dev)
