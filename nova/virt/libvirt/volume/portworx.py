

"""Libvirt volume driver for PX."""
from oslo_log import log as logging
import nova.conf
from nova.virt.libvirt.volume import volume as libvirt_volume
from os_brick.initiator import connector
from nova import utils

LOG = logging.getLogger(__name__)

CONF = nova.conf.CONF


class LibvirtPortworxVolumeDriver(libvirt_volume.LibvirtBaseVolumeDriver):
    """Class PX Libvirt volume Driver

    Implements Libvirt part of volume driver for PX cinder driver.
    Uses the PX connector from the os-brick projects
    """
    def __init__(self, host):
        super(LibvirtPortworxVolumeDriver, self).__init__(host,
                                                          is_block_dev=False)
        self.connector = connector.InitiatorConnector.factory(
           'PORTWORX', utils.get_root_helper(),
           device_scan_attempts=CONF.libvirt.num_iscsi_scan_tries)

    def get_config(self, connection_info, disk_info):
        conf = super(LibvirtPortworxVolumeDriver, self).get_config(
            connection_info, disk_info)
        LOG.info("get_config %s",connection_info)
        conf.source_type = 'block'
        conf.source_path = connection_info['data']['device_path']
        return conf

    def connect_volume(self, connection_info, disk_info):
        LOG.info("connect volume,connection info,%s", connection_info)
        LOG.info("connect volume,disk info,%s", disk_info)
        disk_info['path'] = self.connector.connect_volume(connection_info['data'])
        connection_info['data']['device_path'] = disk_info['path']

    def disconnect_volume(self, connection_info, disk_dev):
        self.connector.disconnect_volume(connection_info['data'], None)
        LOG.info("Disconnected volume %s.", disk_dev)
        super(LibvirtPortworxVolumeDriver, self).disconnect_volume(
            connection_info, disk_dev)
