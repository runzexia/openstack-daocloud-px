# openstack-daocloud-portworx driver	

### nova-compute 

负责产生实例

/nova/virt/libvirt/driver.py

/nova/virt/libvirt/volume/portworx.py



### cinder-volume 

负责除产生实例外其他卷相关操作

/cinder/volume/drivers/daocloud/*



### os-brick

挂卷与解挂卷的util

/os_brick/initiator/connector.py

/os_brick/initiator/connectors/portworx.py

