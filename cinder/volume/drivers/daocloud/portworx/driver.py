import math
from oslo_config import cfg
from oslo_log import log as logging
from cinder import interface
from cinder.volume import driver
import requests
from os_brick.initiator import connector
import json
from six.moves import http_client
from oslo_utils import units
from cinder import exception
from cinder.image import image_utils
from cinder import utils
import requests
import six
CONF = cfg.CONF

LOG = logging.getLogger(__name__)
BLOCK_SIZE = 32


px_opts=[
cfg.StrOpt('px_ip',
               default='',
               help='IP address of PX controller'),
    cfg.StrOpt('px_login',
               default='admin',
               help='Username for PX controller'),
    cfg.StrOpt('px_password',
               default='',
               help='Password for PXcontroller',
               secret=True),
    cfg.StrOpt('px_rest_server_port',
               default='9001',
               help='REST server port.'),
]

CONF.register_opts(px_opts)


@interface.volumedriver
class PortworxDriver(driver.VolumeDriver):

    VERSION = "1.0.0"

    def __init__(self, *args, **kwargs):

        super(PortworxDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(px_opts)
        self.server_ip = self.configuration.px_ip
        self.server_port = self.configuration.px_rest_server_port
        self.connection_properties = {
            'serverIP': self.server_ip,
            'serverPort': self.server_port,
        }
        self.connector = connector.InitiatorConnector.factory(
            'PORTWORX', utils.get_root_helper(),
            self.configuration.num_volume_device_scan_tries
        )
        LOG.info(self.configuration)

    def check_for_setup_error(self):
        LOG.info("check_for_setup_error")

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """
        if refresh:
            self._update_volume_stats()

        return self._stats

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot.

        If volume_type extra specs includes 'replication: <is> True'
        the driver needs to create a volume replica (secondary),
        and setup replication between the newly created volume and
        the secondary volume.
        """

        LOG.info("create_volume_from_snapshot")

    def delete_volume(self, volume):
        LOG.info("delete_volume %s",volume)
        volume_id = volume['provider_id']
        req_vars = {'server_ip': self.server_ip,
                    'server_port': self.server_port,
                    'provider_id': volume_id}

        request = ("http://%(server_ip)s:%(server_port)s"
                   "/v1/osd-volumes/"
                   "%(provider_id)s") % req_vars
        r, response = self._execute_px_get_request(request)
        LOG.info("get response %s" , response)

        if response and 'id'in response[0] and 'attached_on' not in response[0]:
            r, response = self._execute_px_delete_request(request)
            LOG.info("delete volume %s",response)
            if r.status_code != http_client.OK:
                raise exception.VolumeBackendAPIException(data=response)
            elif 'error' in response:
                raise exception.VolumeAttached(data=response)
        elif not response or'id' not in response[0]:
            LOG.info("already deleted")
            return
        else:
            raise exception.VolumeAttached(data=response)

    def delete_snapshot(self, snapshot):
        """Deletes a ScaleIO snapshot."""
        LOG.info("ScaleIO delete snapshot.")
        return self.delete_volume(snapshot)

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        LOG.info("create_snapshot")
        volume_id = snapshot.volume.provider_id
        return self._snapshot_volume(volume_id)

    def local_path(self, volume):
        LOG.info("local_path")

    def clear_download(self, context, volume):
        pass

    def extend_volume(self, volume, new_size):
        LOG.info("extend_volume")

    def manage_existing(self, volume, existing_ref):
        msg = _("Manage existing volume not implemented.")
        LOG.info("manage_existing")

    def revert_to_snapshot(self, context, volume, snapshot):
        """Revert volume to snapshot.

        Note: the revert process should not change the volume's
        current size, that means if the driver shrank
        the volume during the process, it should extend the
        volume internally.
        """
        msg = _("Revert volume to snapshot not implemented.")
        LOG.info("revert_to_snapshot")

    def manage_existing_get_size(self, volume, existing_ref):
        msg = _("Manage existing volume not implemented.")
        LOG.info("manage_existing_get_size")

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset,
                               sort_keys, sort_dirs):
        msg = _("Get manageable volumes not implemented.")
        LOG.info("get_manageable_volumes")

    def unmanage(self, volume):
        pass

    def manage_existing_snapshot(self, snapshot, existing_ref):
        msg = _("Manage existing snapshot not implemented.")
        LOG.info("manage_existing_snapshot")

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        msg = _("Manage existing snapshot not implemented.")
        LOG.info("manage_existing_snapshot_get_size")

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset,
                                 sort_keys, sort_dirs):
        msg = _("Get manageable snapshots not implemented.")
        LOG.info("get_manageable_snapshots")

    def unmanage_snapshot(self, snapshot):
        """Unmanage the specified snapshot from Cinder management."""
        LOG.info("unmanage_snapshot")

    def retype(self, context, volume, new_type, diff, host):
        LOG.info("retype")
        return False, None

        # #######  Interface methods for DataPath (Connector) ########

    def ensure_export(self, context, volume):
        LOG.info("ensure_export")

    def create_export(self, context, volume, connector):
        LOG.info("create_export")

    def create_export_snapshot(self, context, snapshot, connector):
        LOG.info("create_export_snapshot")

    def remove_export(self, context, volume):
        LOG.info("remove_export")

    def remove_export_snapshot(self, context, snapshot):
        LOG.info("remove_export_snapshot")

    def initialize_connection(self, volume, connector, **kwargs):
        """Initializes the connection and returns connection info.

        The portworx driver returns a driver_volume_type of 'portworx'.
        """

        LOG.info("Connector is %s.", connector)
        LOG.info("Volume is %s",volume)
        connection_properties = dict(self.connection_properties)
        connection_properties['provider_id'] = volume['provider_id']
        return {'driver_volume_type': 'portworx',
                'data': connection_properties}

    def initialize_connection_snapshot(self, snapshot, connector, **kwargs):
        """Allow connection from connector for a snapshot."""
        LOG.info("initialize_connection_snapshot")

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector

        :param volume: The volume to be disconnected.
        :param connector: A dictionary describing the connection with details
                          about the initiator. Can be None.
        """
        LOG.info("terminate_connection")

    def terminate_connection_snapshot(self, snapshot, connector, **kwargs):
        """Disallow connection from connector for a snapshot."""
        LOG.info("terminate_connection_snapshot")

    def create_consistencygroup(self, context, group):
        """Creates a consistencygroup.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be created.
        :returns: model_update

        model_update will be in this format: {'status': xxx, ......}.

        If the status in model_update is 'error', the manager will throw
        an exception and it will be caught in the try-except block in the
        manager. If the driver throws an exception, the manager will also
        catch it in the try-except block. The group status in the db will
        be changed to 'error'.

        For a successful operation, the driver can either build the
        model_update and return it or return None. The group status will
        be set to 'available'.
        """
        LOG.info("create_consistencygroup")

    def create_consistencygroup_from_src(self, context, group, volumes,
                                         cgsnapshot=None, snapshots=None,
                                         source_cg=None, source_vols=None):
        """Creates a consistencygroup from source.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be created.
        :param volumes: a list of volume dictionaries in the group.
        :param cgsnapshot: the dictionary of the cgsnapshot as source.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :param source_cg: the dictionary of a consistency group as source.
        :param source_vols: a list of volume dictionaries in the source_cg.
        :returns: model_update, volumes_model_update

        The source can be cgsnapshot or a source cg.

        param volumes is retrieved directly from the db. It is a list of
        cinder.db.sqlalchemy.models.Volume to be precise. It cannot be
        assigned to volumes_model_update. volumes_model_update is a list of
        dictionaries. It has to be built by the driver. An entry will be
        in this format: {'id': xxx, 'status': xxx, ......}. model_update
        will be in this format: {'status': xxx, ......}.

        To be consistent with other volume operations, the manager will
        assume the operation is successful if no exception is thrown by
        the driver. For a successful operation, the driver can either build
        the model_update and volumes_model_update and return them or
        return None, None.
        """
        LOG.info("create_consistencygroup_from_src")

    def delete_consistencygroup(self, context, group, volumes):
        """Deletes a consistency group.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be deleted.
        :param volumes: a list of volume dictionaries in the group.
        :returns: model_update, volumes_model_update

        param volumes is retrieved directly from the db. It is a list of
        cinder.db.sqlalchemy.models.Volume to be precise. It cannot be
        assigned to volumes_model_update. volumes_model_update is a list of
        dictionaries. It has to be built by the driver. An entry will be
        in this format: {'id': xxx, 'status': xxx, ......}. model_update
        will be in this format: {'status': xxx, ......}.

        The driver should populate volumes_model_update and model_update
        and return them.

        The manager will check volumes_model_update and update db accordingly
        for each volume. If the driver successfully deleted some volumes
        but failed to delete others, it should set statuses of the volumes
        accordingly so that the manager can update db correctly.

        If the status in any entry of volumes_model_update is 'error_deleting'
        or 'error', the status in model_update will be set to the same if it
        is not already 'error_deleting' or 'error'.

        If the status in model_update is 'error_deleting' or 'error', the
        manager will raise an exception and the status of the group will be
        set to 'error' in the db. If volumes_model_update is not returned by
        the driver, the manager will set the status of every volume in the
        group to 'error' in the except block.

        If the driver raises an exception during the operation, it will be
        caught by the try-except block in the manager. The statuses of the
        group and all volumes in it will be set to 'error'.

        For a successful operation, the driver can either build the
        model_update and volumes_model_update and return them or
        return None, None. The statuses of the group and all volumes
        will be set to 'deleted' after the manager deletes them from db.
        """
        LOG.info("delete_consistencygroup")

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
        """Updates a consistency group.

        :param context: the context of the caller.
        :param group: the dictionary of the consistency group to be updated.
        :param add_volumes: a list of volume dictionaries to be added.
        :param remove_volumes: a list of volume dictionaries to be removed.
        :returns: model_update, add_volumes_update, remove_volumes_update

        model_update is a dictionary that the driver wants the manager
        to update upon a successful return. If None is returned, the manager
        will set the status to 'available'.

        add_volumes_update and remove_volumes_update are lists of dictionaries
        that the driver wants the manager to update upon a successful return.
        Note that each entry requires a {'id': xxx} so that the correct
        volume entry can be updated. If None is returned, the volume will
        remain its original status. Also note that you cannot directly
        assign add_volumes to add_volumes_update as add_volumes is a list of
        cinder.db.sqlalchemy.models.Volume objects and cannot be used for
        db update directly. Same with remove_volumes.

        If the driver throws an exception, the status of the group as well as
        those of the volumes to be added/removed will be set to 'error'.
        """
        LOG.info("update_consistencygroup")

    def create_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Creates a cgsnapshot.

        :param context: the context of the caller.
        :param cgsnapshot: the dictionary of the cgsnapshot to be created.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :returns: model_update, snapshots_model_update

        param snapshots is retrieved directly from the db. It is a list of
        cinder.db.sqlalchemy.models.Snapshot to be precise. It cannot be
        assigned to snapshots_model_update. snapshots_model_update is a list
        of dictionaries. It has to be built by the driver. An entry will be
        in this format: {'id': xxx, 'status': xxx, ......}. model_update
        will be in this format: {'status': xxx, ......}.

        The driver should populate snapshots_model_update and model_update
        and return them.

        The manager will check snapshots_model_update and update db accordingly
        for each snapshot. If the driver successfully deleted some snapshots
        but failed to delete others, it should set statuses of the snapshots
        accordingly so that the manager can update db correctly.

        If the status in any entry of snapshots_model_update is 'error', the
        status in model_update will be set to the same if it is not already
        'error'.

        If the status in model_update is 'error', the manager will raise an
        exception and the status of cgsnapshot will be set to 'error' in the
        db. If snapshots_model_update is not returned by the driver, the
        manager will set the status of every snapshot to 'error' in the except
        block.

        If the driver raises an exception during the operation, it will be
        caught by the try-except block in the manager and the statuses of
        cgsnapshot and all snapshots will be set to 'error'.

        For a successful operation, the driver can either build the
        model_update and snapshots_model_update and return them or
        return None, None. The statuses of cgsnapshot and all snapshots
        will be set to 'available' at the end of the manager function.
        """
        LOG.info("create_cgsnapshot")

    def delete_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Deletes a cgsnapshot.

        :param context: the context of the caller.
        :param cgsnapshot: the dictionary of the cgsnapshot to be deleted.
        :param snapshots: a list of snapshot dictionaries in the cgsnapshot.
        :returns: model_update, snapshots_model_update

        param snapshots is retrieved directly from the db. It is a list of
        cinder.db.sqlalchemy.models.Snapshot to be precise. It cannot be
        assigned to snapshots_model_update. snapshots_model_update is a list
        of dictionaries. It has to be built by the driver. An entry will be
        in this format: {'id': xxx, 'status': xxx, ......}. model_update
        will be in this format: {'status': xxx, ......}.

        The driver should populate snapshots_model_update and model_update
        and return them.

        The manager will check snapshots_model_update and update db accordingly
        for each snapshot. If the driver successfully deleted some snapshots
        but failed to delete others, it should set statuses of the snapshots
        accordingly so that the manager can update db correctly.

        If the status in any entry of snapshots_model_update is
        'error_deleting' or 'error', the status in model_update will be set to
        the same if it is not already 'error_deleting' or 'error'.

        If the status in model_update is 'error_deleting' or 'error', the
        manager will raise an exception and the status of cgsnapshot will be
        set to 'error' in the db. If snapshots_model_update is not returned by
        the driver, the manager will set the status of every snapshot to
        'error' in the except block.

        If the driver raises an exception during the operation, it will be
        caught by the try-except block in the manager and the statuses of
        cgsnapshot and all snapshots will be set to 'error'.

        For a successful operation, the driver can either build the
        model_update and snapshots_model_update and return them or
        return None, None. The statuses of cgsnapshot and all snapshots
        will be set to 'deleted' after the manager deletes them from db.
        """
        LOG.info("delete_cgsnapshot")


    def get_pool(self, volume):
        """Return pool name where volume reside on.

        :param volume: The volume hosted by the driver.
        :returns: name of the pool where given volume is in.
        """
        LOG.info("get_pool")
        return None

    def migrate_volume(self, context, volume, host):
        LOG.info("migrate_volume")
        return (False, None)

    def accept_transfer(self, context, volume, new_user, new_project):
        LOG.info("accept_transfer")

    def _execute_px_get_request(self, request):
        r = requests.get(
            request)
        r = self._check_response(r, request)
        response = r.json()
        return r, response

    def _execute_px_delete_request(self, request):
        r = requests.delete(
            request)
        r = self._check_response(r, request)
        response = r.json()
        return r, response

    def _check_response(self, response, request, is_get_request=True,
                        params=None):
        if response.status_code != http_client.OK:
            level = logging.ERROR

            LOG.log(level, "REST Request: %s with params %s",
                request,
                json.dumps(params))
            LOG.log(level, "REST Response: %s with data %s",
                     response.status_code,
                        response.text)
        return response

    def _update_volume_stats(self):
        stats = {}
        stats['volume_backend_name'] = 'portworx'
        stats['driver_version'] = self.VERSION
        stats['storage_protocol'] = 'portworx'
        stats['vendor_name'] = 'DaoCloud'
        stats['free_capacity_gb'] = 0
        stats['total_capacity_gb'] = 0

        req_vars = {'server_ip': self.server_ip,
                    'server_port': self.server_port}

        request = ("http://%(server_ip)s:%(server_port)s"
                   "/v1/cluster/enumerate") % req_vars
        LOG.info(request)

        r, response = self._execute_px_get_request(request)
        # LOG.info("Query  stats response: %s.", response)
        total_capacity_b = 0
        free_capacity_b = 0
        for res in response["Nodes"]:
            if 'NodeData' in res and res['NodeData'] != None:
                total_capacity_b += res["NodeData"]["storage_stats"]["DiskTotal"]
                free_capacity_b +=res["NodeData"]["storage_stats"]["DiskAvail"]
        stats['total_capacity_gb'] = self._convert_b_to_gib(total_capacity_b)
        stats['free_capacity_gb'] = self._convert_b_to_gib(free_capacity_b)
        self._stats = stats

    @staticmethod
    def _convert_b_to_gib(size):
        return int(math.ceil(float(size) / units.Gi))

    def _execute_px_post_request(self, params, request):
        r = requests.post(
            request,
            data=json.dumps(params))
        r = self._check_response(r, request, False, params)
        response = None
        try:
            response = r.json()
        except ValueError:
            response = None
        return r, response

    def create_volume(self, volume):
        """Creates a PX volume."""
        volname = volume.id
        volume_size_b = volume["size"]*units.Gi
        params = {
            "locator":{
                "name":volname
            },
            "spec":{
                "size":volume_size_b,
                "format": 0,
                "block_size": BLOCK_SIZE*units.Ki,
                "ha_level": 1,
                "cos": 1,
                "io_priority": "medium",
                "dedupe": False,
                "snapshot_interval": 0,
                "shared": False,
                "replica_set": {}
            }
        }
        LOG.info("Params for add volume request: %s.", params)
        req_vars = {'server_ip': self.server_ip,
                    'server_port': self.server_port}
        request = ("http://%(server_ip)s:%(server_port)s"
                   "/v1/osd-volumes") % req_vars
        r, response = self._execute_px_post_request(params,request)
        if r.status_code != http_client.OK or not response.has_key('id'):
            msg = ("Error creating volume: "+str(response))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        LOG.info("Created volume %(volname)s, volume id %(volid)s.",
                 {'volname': volname, 'volid': volume.id})

        return {'provider_id': response['id'], 'size': volume["size"]}

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""
        LOG.info("portworx copy_image_to_volume volume: %(vol)s image service: "
                 "%(service)s image id: %(id)s.",
                 {'vol': volume,
                  'service': six.text_type(image_service),
                  'id': six.text_type(image_id)})

        try:
            image_utils.fetch_to_raw(context,
                                     image_service,
                                     image_id,
                                     self._px_attach_volume(volume),
                                     BLOCK_SIZE,
                                     size=volume['size'])

        finally:
            self._px_detach_volume(volume)

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image."""
        LOG.info("Portworx copy_volume_to_image volume: %(vol)s image service: "
                 "%(service)s image meta: %(meta)s.",
                 {'vol': volume,
                  'service': six.text_type(image_service),
                  'meta': six.text_type(image_meta)})
        try:
            image_utils.upload_volume(context,
                                      image_service,
                                      image_meta,
                                      self._px_attach_volume(volume))

        finally:
            self._px_detach_volume(volume)

    def _px_attach_volume(self, volume):
        """Call connector.connect_volume() and return the path. """
        LOG.info("Calling os-brick to attach PX volume.")
        connection_properties = dict(self.connection_properties)
        connection_properties['provider_id'] = volume.provider_id
        device_info = self.connector.connect_volume(connection_properties)
        return device_info

    def _px_detach_volume(self, volume):
        """Call the connector.disconnect() """
        LOG.info("Calling os-brick to detach PX volume.")
        connection_properties = dict(self.connection_properties)
        connection_properties['provider_id'] = volume.provider_id
        self.connector.disconnect_volume(connection_properties, volume)

    def _snapshot_volume(self, vol_id):
        LOG.info("Snapshot volume %(vol)s.",
                 {'vol': vol_id})
        params = {
            "id": vol_id,
            "locator": {}
        }
        req_vars = {'server_ip': self.server_ip,
                    'server_port': self.server_port}
        request = ("http://%(server_ip)s:%(server_port)s"
                   "/v1/osd-snapshot") % req_vars
        r, response = self._execute_px_post_request(params, request)
        LOG.info("response %s",response)
        if r.status_code != http_client.OK:
            msg = (_("Failed creating snapshot for volume %(volname)s: "
                     "%(response)s.") %
                   {'volname': vol_id,
                    'response': response['message']})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return {'provider_id': response['volume_create_response']['id']}
