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

  public function createConfiguration() {
    $msg = [];

    // create contact types
    $msg[] = "=== Checking Contact Sub Types ===";
    $sql = "
      select
        distinct t.contact_sub_type
      from
        tmpbeuc_orgs t
      where
        not exists (
          select * from civicrm_contact_type ct where ct.name = t.contact_sub_type
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

    // create tags
    $msg[] = "=== Checking Tags ===";
    $sql = "
      select
        distinct tag
      from
        tmpbeuc_orgs t
      where
        not exists (
          select * from civicrm_tag ct where ct.name = t.tag
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

    // create custom group press
    $msg[] = "=== Checking Custom Group Press ===";
    $result = civicrm_api3('CustomGroup', 'get', ['title' => "Press", 'sequential' => 1]);
    if ($result['count'] == 0) {
      $result = civicrm_api3('CustomGroup', 'create', [
        'title' => "Press",
        'extends' => "Organization",
        'extends_entity_column_value' => ['Press_Organisation'],
        'name' => "press",
        'collapse_display' => 0,
        'style' => "Inline",
        'table_name' => "civicrm_value_press",
        'sequential' => 1,
      ]);

      $msg[] = 'Create custom group Press';
    }

    $groupID = $result['values'][0]['id'];

    // create custom fields for press
    $msg[] = "=== Checking Custom Fields for Press ===";
    $result = civicrm_api3('CustomField', 'get', ['label' => "Media Country"]);
    if ($result['count'] == 0) {
      $result = civicrm_api3('CustomField', 'create', [
        'custom_group_id' => $groupID,
        'label' => "Media Country",
        'data_type' => "Country",
        'is_searchable' => 1,
        'is_active' => 1,
        'column_name' => "media_country",
        'html_type' => "Select",
      ]);

      $msg[] = 'Create custom field Media Country';
    }

    // create the option group for custom field media type
    $result = civicrm_api3('OptionGroup', 'get', ['name' => "media_type_list", 'sequential' => 1]);
    if ($result['count'] == 0) {
      $result = civicrm_api3('OptionGroup', 'create', [
        "name" => "media_type_list",
        "title" => "Media Type List",
        "is_reserved" => "0",
        "is_active" => "1",
        "is_locked" => "0",
        'sequential' => 1,
      ]);
    }
    $listID = $result['values'][0]['id'];

    // add the option items
    $this->createOptionListItem($listID, 1, 'News Agency'); 
    $this->createOptionListItem($listID, 2, 'Newspaper');
    $this->createOptionListItem($listID, 3, 'Magazine');
    $this->createOptionListItem($listID, 4, 'TV');
    $this->createOptionListItem($listID, 5, 'Radio');
    $this->createOptionListItem($listID, 6, 'Weekly');
    $this->createOptionListItem($listID, 7, 'Online');
    $this->createOptionListItem($listID, 8, 'Blog');
    $this->createOptionListItem($listID, 9, 'Broadcaster');

    // create custom field for media type
    $result = civicrm_api3('CustomField', 'get', ['label' => "Media Type"]);
    if ($result['count'] == 0) {
      $result = civicrm_api3('CustomField', 'create', [
        'custom_group_id' => $groupID,
        'label' => "Media Type",
        'name' => 'media_type',
        'data_type' => 'Int',
        'html_type' => 'Radio',
        'is_required' => '0',
        'is_searchable' => '1',
        'is_search_range' => '0',
        'is_active' => '1',
        'options_per_line' => '1',
        'column_name' => 'media_type',
        'option_group_id' => $listID,
      ]);

      $msg[] = 'Create custom field Media Type';
    }

    return $msg;
  }

  private function createOptionListItem($listID, $id, $label) {
    $name = str_replace(' ', '-', strtolower($label));
    $result = civicrm_api3('OptionValue', 'get', ['option_group_id' => $listID, 'name' => $name, 'sequential' => 1]);
    if ($result['count'] == 0) {
      civicrm_api3('OptionValue', 'create', [
        'option_group_id' => $listID,
        'label' => $label,
        'value' => $id,
        'name' => $name,
        'is_active' => '1'
      ]);
    }

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
      $sql = "select count(*) from tmpbeuc_orgs where ifnull(status, '') = ''";
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
    $numItemsPerQueueRun = 20;

    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      // count the number of items to import
      $sql = "select count(*) from tmpbeuc_pers where ifnull(status, '') = ''";
      $numItems = CRM_Core_DAO::singleValueQuery($sql);

      // fill the queue
      $totalQueueRuns = ($numItems / $numItemsPerQueueRun) + 1;
      for ($i = 0; $i < $numItems; $i++) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importPersonTask'], [$numItemsPerQueueRun]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import Persons');
    }

    return $msg;
  }

  public static function importOrganizationTask(CRM_Queue_TaskContext $ctx, $limit) {
    static $custom_field_media_type = '';
    static $custom_field_media_country = '';

    // get custom field id's
    if ($custom_field_media_type == '') {
      $custom_field_media_type = 'custom_' . civicrm_api3('CustomField', 'getsingle', ['return' => ["id"], 'custom_group_id' => "press", 'name' => "media_type"])['id'];
    }
    if ($custom_field_media_country == '') {
      $custom_field_media_country = 'custom_' . civicrm_api3('CustomField', 'getsingle', ['return' => ["id"], 'custom_group_id' => "press", 'name' => "media_country"])['id'];
    }

    $sql = "
      select
        o.*,
        t.id as tag_id,
        c.id as country_id
      from
        tmpbeuc_orgs o
      left outer join 
        civicrm_tag t on o.tag = t.name
      left outer join 
        civicrm_country c on o.country_iso_code = c.iso_code
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

        if ($dao->contact_sub_type) {
          $params['contact_sub_type'] = $dao->contact_sub_type;
        }

        if ($dao->media_type_id) {
          $params[$custom_field_media_type] = $dao->media_type_id;
        }

        if ($dao->press_country_iso_code) {
          $params[$custom_field_media_country] = $dao->press_country_iso_code;
        }

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

        $updateSQL = "update tmpbeuc_orgs set status = 'O' where external_identifier = '" . $dao->external_identifier . "'";
        CRM_Core_DAO::executeQuery($updateSQL);
      }
    }

    return TRUE;
  }

  public static function importPersonTask(CRM_Queue_TaskContext $ctx, $limit) {
    $sql = "
      select
        p.*
        , ov.value prefix_id
      from
        tmpbeuc_pers p
      left outer join
        civicrm_option_value ov on p.title = ov.name and option_group_id = 6
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
        $params['contact_type'] = 'Individual';
        $params['first_name'] = $dao->first_name;
        $params['last_name'] = $dao->last_name;
        $params['source'] = $dao->source;
        $params['employer_id'] = self::getOrganizationFromExternalID($dao->employer_id);
        $params['job_title'] = $dao->job_title;
        $params['preferred_language'] = $dao->preferred_language;

        // in civi: female = 1, male = 2 (in BEUC it's the other way around)
        if ($dao->gender == 1) {
          $params['gender_id'] = 2;
        }
        elseif ($dao->gender == 2) {
          $params['gender_id'] = 1;
        }

        // prefix (Mr., Mrs...)
        if ($dao->prefix_id) {
          $params['prefix_id'] = $dao->prefix_id;
        }
        elseif ($dao->gender == 1) {
          $params['prefix_id'] = 3; // Mr.
        }
        elseif ($dao->gender == 2) {
          $params['prefix_id'] = 1; // Mrs.
        }

        // add twitter link
        if ($dao->twitter) {
          $params['api.Website.create'] = [
            'url' => $dao->twitter,
            'website_type_id' => 11,
          ];
        }

        // add note
        if ($dao->comments) {
          $params['api.Note.create'] = [
            'note' => $dao->comments,
            'entity_table' => 'civicrm_contact',
            'subject' => 'imported note',
          ];
        }

        // add email
        if ($dao->email) {
          $params['api.Email.create'] = [
            'email' => $dao->email,
            'location_type_id' => 2, //work
            'is_primary' => 1,
          ];
        }

        $c = civicrm_api3('Contact', 'create', $params);

        // add phone numbers
        self::addPhoneNumbers($c['id'], $dao->external_identifier);

        $updateSQL = "update tmpbeuc_pers set status = 'O' where external_identifier = '" . $dao->external_identifier . "'";
        CRM_Core_DAO::executeQuery($updateSQL);
      }
    }

    return TRUE;
  }

  public static function addPhoneNumbers($contactID, $external_identifier) {
    $sql = "
      select
        external_identifier, phone_type, max(phone) phone_number
      from
        tmpbeuc_phones
      where
        external_identifier = %1
      group by
        external_identifier, phone_type
    ";
    $sqlParams = [
      1 => [$external_identifier, 'String'],
    ];
    $dao = CRM_Core_DAO::executeQuery($sql, $sqlParams);

    while ($dao->fetch()) {
      $params = [
        'contact_id' => $contactID,
        'phone' => substr($dao->phone_number, 0, 32),
        'location_type_id' => 2, // work
        'phone_type_id' => $dao->phone_type == 'mobile' ? 2 : 1,
      ];
      civicrm_api3('Phone', 'create', $params);
    }
  }

  public static function getOrganizationFromExternalID($external_identifier) {
    $retval = '';

    if ($external_identifier) {
      $params = [
        'external_identifier' => $external_identifier,
        'contact_type' => 'Organization',
        'sequential' => 1,
      ];
      $c = civicrm_api3('Contact', 'get', $params);
      if ($c['count'] > 0) {
        $retval = $c['values'][0]['id'];
      }
    }

    return $retval;
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

  public static function onEnd(CRM_Queue_TaskContext $ctx) {
    CRM_Core_Session::setStatus('All tasks in queue are executed', 'Queue', 'success');
  }
}