<?php


class CRM_Beucimport_Helper {
  private $queueName = 'beucimport';
  private $queue;

  public function deleteQueue() {
    $this->createQueue();

    $msg = $this->queue->numberOfItems() . ' item(s) deleted';
    $this->queue->deleteQueue();

    return $msg;
  }

  public function checkConfiguration() {
    $msg = [];

    // check contact sub types
    $sql = "
      select
        distinct t.contact_sub_type
      from
        tmp_orgs t
      where
        not exists (
          select * from civicrm_contact_type ct where ct.name collate utf8_general_ci = t.contact_sub_type collate utf8_general_ci
        )
        and ifnull(t.contact_sub_type, '') <> ''
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      $msg[] = 'Contact subtype "' . $dao->contact_sub_type . '" does not exist';
    }

    // check missing tags
    $sql = "
      select
        distinct tag
      from
        tmp_orgs t
      where
        not exists (
          select * from civicrm_tag ct where ct.name collate utf8_general_ci = t.tag collate utf8_general_ci
        )
        and ifnull(t.tag, '') <> ''
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      $msg[] = 'Tag "' . $dao->tag . '" does not exist';
    }

    if (count($msg) == 0) {
      $msg[] = 'OK';
    }

    return $msg;
  }

  public function createConfiguration() {
    $msg = [];

    // create missing contact types
    $sql = "
      select
        distinct t.contact_sub_type
      from
        tmp_orgs t
      where
        not exists (
          select * from civicrm_contact_type ct where ct.name collate utf8_general_ci = t.contact_sub_type collate utf8_general_ci
        )
        and ifnull(t.contact_sub_type, '') <> ''
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      $params = [
        'name' => $dao->contact_sub_type,
        'label' => str_replace('_', ' ', $dao->contact_sub_type),
        'parent_id' => 'Organization',
        'is_active' => 1,
      ];
      civicrm_api3('ContactType', 'create', $params);
      $msg[] = 'Create subtype ' . $dao->contact_sub_type;
    }

    // create missing tags
    $sql = "
      select
        distinct tag
      from
        tmp_orgs t
      where
        not exists (
          select * from civicrm_tag ct where ct.name collate utf8_general_ci = t.tag collate utf8_general_ci
        )
        and ifnull(t.tag, '') <> ''
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      $params = [
        'name' => $dao->tag,
        'used_for' => 'civicrm_contact',
      ];
      civicrm_api3('Tag', 'create', $params);

      $msg[] = 'Create tag ' . $dao->tag;
    }

    if (count($msg) == 0) {
      $msg[] = 'OK';
    }

    return $msg;

  }

  public function importOrganizations() {
    $numItemsPerQueueRun = 20;

    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      // count the number of items to import
      $sql = "select count(*) from tmp_orgs where ifnull(status, '') = ''";
      $numItems = CRM_Core_DAO::singleValueQuery($sql);

      // fill the queue
      $totalQueueRuns = ($numItems / $numItemsPerQueueRun) + 1;
      for ($i = 0; $i < $numItems; $i++) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importOrganizationTask'], [$numItemsPerQueueRun]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import Organizations');
    }

    return $msg;
  }

  public function importPersons() {

  }

  public function importPhoneNumbers() {

  }

  private function createQueue() {
    $this->queue = CRM_Queue_Service::singleton()->create([
      'type' => 'Sql',
      'name' => $this->queueName,
      'reset' => FALSE, //do not flush queue upon creation
    ]);
  }

  private function runQueue($title) {
    $runner = new CRM_Queue_Runner([
      'title' => $title,
      'queue' => $this->queue,
      'errorMode'=> CRM_Queue_Runner::ERROR_CONTINUE,
      'onEnd' => ['CRM_Beucimport_Helper', 'onEnd'],
      'onEndUrl' => CRM_Utils_System::url('civicrm/beucimport', 'reset=1'),
    ]);

    $runner->runAllViaWeb();
  }

  public static function importOrganizationTask(CRM_Queue_TaskContext $ctx, $limit) {
    $sql = "
      select
        o.*,
        t.id as tag_id,
        c.id as country_id
      from
        tmp_orgs o
      left outer join 
        civicrm_tag t on o.tag collate utf8_general_ci = t.name collate utf8_general_ci
      left outer join 
        civicrm_country c on o.country_iso_code collate utf8_general_ci = c.iso_code collate utf8_general_ci
      where 
        ifnull(status, '') = ''
      limit
        0, $limit
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);

    while ($dao->fetch()) {
      // see if we have this contact
      $params = [
        'sequential' => 1,
        'external_identifier' => $dao->external_identifier,
      ];
      $c = civicrm_api3('Contact', 'get', $params);

      if ($c['count'] == 0) {
        // does not exist, add additional fields
        $params['contact_type'] = 'Organization';
        $params['organization_name'] = $dao->organization_name;
        $params['source'] = $dao->source;

        // add website
        if ($dao->website2) {
          $params['api.Website.create'] = [
            'website_type_id' => 1,
            'url' => $dao->website2,
          ];
        }

        // add tag
        if ($dao->tag_id) {
          $params['api.EntityTag.create'] = [
            'entity_table' => 'civicrm_contact',
            'tag_id' => $dao->tag_id,
          ];
        }

        // add address
        $params['api.Address.create'] = [
          'location_type_id' => 2,
          'is_primary' => 1,
          'street_address' => $dao->street_address,
          'supplemental_address_1' => $dao->supplemental_address_1,
          'city' => $dao->city,
          'postal_code' => $dao->postal_code,
          'country_id' => $dao->country_id,
        ];

        civicrm_api3('Contact', 'create', $params);

        $updateSQL = "update tmp_orgs set status = 'OK' where external_identifier = '" . $dao->external_identifier . "'";
        CRM_Core_DAO::executeQuery($updateSQL);
      }
    }

    return TRUE;
  }

  public static function onEnd(CRM_Queue_TaskContext $ctx) {
    CRM_Core_Session::setStatus('All tasks in queue are executed', 'Queue', 'success');
  }
}