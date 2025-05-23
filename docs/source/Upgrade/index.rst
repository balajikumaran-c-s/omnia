Upgrade Omnia OIM
=====================

To upgrade the Omnia version 1.7 to version 1.7.1 on your OIM, you can use the ``upgrade_oim.yml`` playbook in Omnia 1.7.1. This ensures that your OIM is running the latest version and includes any new features and improvements that are available.

.. note::

    * Before initiating upgrade, ensure that the OIM has a stable internet connection to avoid intermittent issues caused by poor network connectivity.
    * After upgrading the Omnia OIM running on a `supported OS <../Overview/SupportMatrix/OperatingSystems/index.html>`_, the ``input/software_config.json`` file remains in its default state. This enables users to install the default software versions on a new cluster.
    * After upgrading your OIM, ensure that the jinja2 version on the login nodes is also updated to 3.1.6. To update the jinja2 software version, run the following command: ::

        pip install jinja2==3.1.6
    
    * As part of the upgrade process, Omnia upgrades the grafana version on the cluster to 11.4.1 from 8.3.2.

**Tasks performed by the** ``upgrade_oim.yml`` **playbook**

The ``upgrade_oim.yml`` playbook performs the following tasks:

* Validates whether upgrade can be performed on the Omnia OIM.
* Takes backup of the Kubernetes etcd database, TimescaleDB, and MySQLDB at the backup location specified by the user.
* Imports input parameters from provided source code path of already installed Omnia version.
* Upgrades the software version of nerdctl, jinja2, and kubernetes on the OIM.
* Upgrades ``omnia_telemetry`` binaries on nodes where the telemetry service is running.

**Pre-check before Upgrade**

If you have deployed a telemetry service in your Kubernetes cluster, it is important to ensure that the cluster is running properly before you initiate the upgrade process. As part of the upgrade pre-check, Omnia verifies if there are any issues with the cluster, such as non-running pods, LoadBalancer services without external IPs, or unbounded PVCs. If any of these issues are detected, you will need to address them before you can proceed with the upgrade.

**Steps to be performed for Upgrade**

To upgrade the Omnia OIM, do the following:

1. Clone the Omnia 1.7.1 source code to your OIM using the following command: ::

    git clone https://github.com/dell/omnia.git -b v1.7.1

2. Execute the ``prereq.sh`` script using the following command: ::

    cd omnia
    ./prereq.sh

3. Use the following command to activate the Omnia virtual environment: ::

    source /opt/omnia/omnia171_venv/bin/activate

4. Update the ``omnia/upgrade/upgrade_config.yml`` file with the following details:

    +-----------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
    |  Variable Name              | Description                                                                                                                                     |
    +=============================+=================================================================================================================================================+
    | ``installed_omnia_path``    | * This variable points to the currently installed Omnia 1.7 source code directory.                                                              |
    |      Required               | * **Example**: ``/root/omnia17/omnia``                                                                                                          |
    |                             | .. note:: Verify that the directory has not been altered since the last execution of ``discovery_provision.yml`` and ``omnia.yml`` playbooks.   |
    +-----------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``backup_location``         | * This variable points to the directory where the Omnia OIM backup is stored during the upgrade process.                                        |
    |    Optional                 | * User must create this directory before running ``upgrade_oim.yml`` playbook and provide the complete path of that directory.                  |
    |                             | * If the specified directory doesn't exist, backups will be taken at ``/opt/omnia/backup_before_upgrade``                                       |
    |                             | * **Example**: ``/opt/omnia/upgrade_backup``                                                                                                    |
    +-----------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+

5. Finally, execute the ``upgrade_oim.yml`` playbook using the following command: ::

    cd upgrade
    ansible-playbook upgrade_oim.yml

.. caution::

    If ``upgrade_oim.yml`` execution fails while upgrading Kubernetes, you can rollback to Kubernetes version 1.29.5 and restore the old backed-up data using the ``restore_oim.yml`` playbook. To restore, do the following:

        1. Deactivate the Omnia 1.7.1 virtual environment using the ``deactivate`` command.
        
        2. Activate the 1.7 Omnia virtual environment using the ``source /opt/omnia/omnia17_venv/bin/activate`` command.

        3. Execute the ``restore_oim.yml`` playbook using the following command: ::

            cd upgrade
            ansible-playbook restore_oim.yml

**Post Upgrade**

Things to keep in mind after the OIM has been upgraded successfully:

* To use Omnia 1.7.1 features, ensure to execute all the playbooks from within the Omnia 1.7.1 virtual environment. To activate the virtual environment, use the following command: ::

    source /opt/omnia/omnia171_venv/bin/activate

* After the upgrade, verify that the correct input values have been imported during the upgrade process. Check the files under the ``omnia/input`` directory.

* After upgrading your Omnia OIM to version 1.7.1, the new cluster configuration features added in this version won’t work with any of your existing clusters. These new features will only be available when you create new clusters on RHEL/Rocky Linux 8.8, Ubuntu 22.04 or 24.04 platforms, using Omnia 1.7.1 source code.