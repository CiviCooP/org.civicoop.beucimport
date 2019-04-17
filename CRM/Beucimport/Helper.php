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

    $result = civicrm_api3('CustomField', 'get', ['label' => "EU Media"]);
    if ($result['count'] == 0) {
      $result = civicrm_api3('CustomField', 'create', [
        'custom_group_id' => $groupID,
        'label' => "EU Media",
        'data_type' => "Boolean",
        'is_searchable' => 1,
        'is_active' => 1,
        'column_name' => "eu_media",
        'html_type' => "Radio",
      ]);

      $msg[] = 'Create custom field Media Country';
    }

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

  public function setupEU() {
    $orgSubTypes = ['EC DG', 'EC Unit', 'EC Dir', 'EC Cab', 'EP Secretariat General', 'EP Committee'];
    foreach ($orgSubTypes as $subType) {
      $params = [
        'sequential' => 1,
        'name' => str_replace(' ', '_', $subType),
      ];
      $result = civicrm_api3('ContactType', 'get', $params);
      if ($result['count'] == 0) {
        // create sub type
        $params['label'] = $subType;
        $params['parent_id'] = 3;
        $params['is_active'] = 1;
        civicrm_api3('ContactType', 'create', $params);
      }
    }

    // delete disabled relationship types
    $sql = "delete from civicrm_relationship_type where is_reserved is null and is_active = 0";
    CRM_Core_DAO::executeQuery($sql);

    // create EC and EP relationship types
    $relTypes = [
      ['Commissioner', 'EC_Cab'],
      ['Director', 'EC_Dir'],
      ['Director General', 'EC_DG'],
      ['Head of Unit', 'EC_Unit'],
      ['Deputy Head of Unit', 'EC_Unit'],
      ['Head of Cabinet', 'EC_Cab'],
      ['Deputy Head of Cabinet', 'EC_Cab'],
      ['Deputy Director General', 'EC_DG'],
      ['Working at', ''],
      ['Administrator', 'EP_Committee'],
      ['Adviser', 'EP_Committee'],
      ['Working at', 'EP_Committee'],
      ['Secretariat', 'EP_Committee'],
      ['Member', 'EP_Committee'],
      ['Substitute', 'EP_Committee'],
      ['Vice-Chair', 'EP_Committee'],
      ['Chair', 'EP_Committee'],
      ['Coordinator', 'EP_Committee'],
      ['Political Adviser', 'EP_Committee'],
    ];

    foreach ($relTypes as $relType) {
      // see if the rel type exists
      $sql = "select id from civicrm_relationship_type where name_a_b = %1";
      $sqlParams = [1 => [$relType[0], 'String']];
      if (CRM_Core_DAO::singleValueQuery($sql, $sqlParams)) {
        // already exists
      }
      else {
        $params = [
          'name_a_b' => $relType[0],
          'label_a_b' => $relType[0],
          'name_b_a' => $relType[0],
          'label_b_a' => $relType[0],
          'contact_type_a' => 'Individual',
          'contact_type_b' => 'Organization',
          'contact_sub_type_b' => $relType[1],
          'is_active' => 1,
        ];

        civicrm_api3('RelationshipType', 'create', $params);
      }
    }

    // create group for errors
    $groupName = 'ec_check';
    $groupTitle = 'EC Check manually';
    $groupDescription = 'Invalid relationship during import';
    $params = [
      'name' => $groupName,
    ];
    $grp = civicrm_api3('Group', 'get', $params);
    if ($grp['count'] == 0) {
      $params['is_active'] = 1;
      $params['title'] = $groupTitle;
      $params['description'] = $groupDescription;
      civicrm_api3('Group', 'create', $params);
    }

    return 'OK';
  }

  public function importGroups() {
    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      $sql = "select * from tmpbeuc_groups";
      $dao = CRM_Core_DAO::executeQuery($sql);

      // fill the queue with group id's
      while ($dao->fetch()) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importGroupTask'], [$dao->gid]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import Groups');
    }

    return $msg;
  }

  public function importGroupCorrections() {
    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      $sql = "select * from tmpbeuc_group_member_corrections";
      $dao = CRM_Core_DAO::executeQuery($sql);

      // fill the queue with group id's
      while ($dao->fetch()) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importGroupCorrectionTask'], [$dao->gid]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import Groups');
    }

    return $msg;
  }

  public function importOrganizations() {
    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      $sql = "select id from tmpbeuc_orgs";
      $dao = CRM_Core_DAO::executeQuery($sql);

      while ($dao->fetch()) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importOrganizationTask'], [$dao->id]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import Organizations');
    }

    return $msg;
  }

  public function importPersons() {
    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      // add the id's to the queue
      $sql = "select id from tmpbeuc_pers";
      $dao = CRM_Core_DAO::executeQuery($sql);

      while ($dao->fetch()) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importPersonTask'], [$dao->id]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import Persons');
    }

    return $msg;
  }

  public static function importOrganizationTask(CRM_Queue_TaskContext $ctx, $id) {
    static $custom_field_media_type = '';
    static $custom_field_media_country = '';
    static $custom_field_eu_media = '';

    // get custom field id's
    if ($custom_field_media_type == '') {
      $custom_field_media_type = 'custom_' . civicrm_api3('CustomField', 'getsingle', ['return' => ["id"], 'custom_group_id' => "press", 'name' => "media_type"])['id'];
    }
    if ($custom_field_media_country == '') {
      $custom_field_media_country = 'custom_' . civicrm_api3('CustomField', 'getsingle', ['return' => ["id"], 'custom_group_id' => "press", 'name' => "media_country"])['id'];
    }
    if ($custom_field_eu_media == '') {
      $custom_field_eu_media = 'custom_' . civicrm_api3('CustomField', 'getsingle', ['return' => ["id"], 'custom_group_id' => "press", 'name' => "eu_media"])['id'];
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
        o.id = $id
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);

    if ($dao->fetch()) {
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

        if ($dao->media_country_iso_code) {
          if (strtolower($dao->media_country_iso_code) == 'ye') {
            // yemen is a fake media country to mark the press org to be "eu wide"
            $params[$custom_field_eu_media] = 1;
          }
          else {
            $params[$custom_field_media_country] = $dao->media_country_iso_code;
          }
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

  public static function importGroupCorrectionTask(CRM_Queue_TaskContext $ctx, $gid) {
    // select the group correction
    $sql = "select * from tmpbeuc_group_member_corrections where gid = $gid";
    $groupCorrection = CRM_Core_DAO::executeQuery($sql);
    $groupCorrection->fetch();

    // add_contacts_to can contain more than 1 id
    $targetGIDs = explode(', ', $groupCorrection->add_contacts_to);
    foreach ($targetGIDs as $targetGID) {
      // get the civicrm group id of this gid
      $civiGroupIDgid = CRM_Core_DAO::singleValueQuery("select g.id from tmpbeuc_groups tg inner join civicrm_group g on tg.`group` = g.title where tg.gid = " . $targetGID);

      // select the group members
      $sql = "select * from tmpbeuc_group_members where gid = $gid";
      $dao = CRM_Core_DAO::executeQuery($sql);
      while ($dao->fetch()) {
        // lookup the contact
        $result = civicrm_api3('Contact', 'get', ['external_identifier' => 'uid_' . $dao->uid, 'sequential' => 1]);
        if ($result['count'] == 0) {
          watchdog('import group member', 'uid ' . $dao->uid . 'not found');
        }
        else {
          civicrm_api3('GroupContact', 'create', [
            'group_id' => $civiGroupIDgid,
            'contact_id' => $result['values'][0]['id'],
          ]);
        }
      }
    }

    return TRUE;
  }

  public static function importGroupTask(CRM_Queue_TaskContext $ctx, $gid) {
    // select the name of the group
    $sql = "select `group` from tmpbeuc_groups where gid = $gid";
    $groupName = CRM_Core_DAO::singleValueQuery($sql);

    // see if we have that group
    $result = civicrm_api3('Group', 'get', ['title' => $groupName, 'sequential' => 1]);
    if ($result['count'] == 0) {
      // create the group
      $result = civicrm_api3('Group', 'create', ['title' => $groupName, 'sequential' => 1]);
    }
    $groupID = $result['values'][0]['id'];

    // select the group members
    $sql = "select * from tmpbeuc_group_members where gid = $gid";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      // lookup the contact
      $result = civicrm_api3('Contact', 'get', ['external_identifier' => 'uid_' . $dao->uid, 'sequential' => 1]);
      if ($result['count'] == 0) {
        watchdog('import group member', 'uid ' . $dao->uid . 'not found');
      }
      else {
        civicrm_api3('GroupContact', 'create', [
          'group_id' => $groupID,
          'contact_id' => $result['values'][0]['id'],
        ]);
      }
    }

    return TRUE;
  }

  public static function importPersonTask(CRM_Queue_TaskContext $ctx, $id) {
    $sql = "
      select
        p.*
        , ov.value prefix_id
        , c.id as country_id
      from
        tmpbeuc_pers p
      left outer join
        civicrm_option_value ov on p.title = ov.name and option_group_id = 6
      left outer join 
        civicrm_country c on p.country_iso_code = c.iso_code        
      where 
         p.id = $id
    ";
    $dao = CRM_Core_DAO::executeQuery($sql);

    if ($dao->fetch()) {
      // skip if it's empty
      if (!$dao->first_name && !$dao->last_name && !$dao->email) {
        $updateSQL = "update tmpbeuc_pers set status = 'O' where external_identifier = '" . $dao->external_identifier . "'";
        CRM_Core_DAO::executeQuery($updateSQL);
        return TRUE;
      }

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

        // if no name, fill in email as first name
        if (!$dao->first_name && !$dao->last_name) {
          $params['first_name'] = $dao->email;
        }

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

        // add the address if it's different from the employer address
        if ($dao->aid) {
          $createNewAddress = FALSE;

          // get the address of the employer
          $emplID = $params['employer_id'] ? $params['employer_id'] : 0;
          $employerAddress = civicrm_api3('Address', 'get', ['sequential' => 1, 'contact_id' => $emplID, 'is_primary' => 1]);
          if ($employerAddress['count'] > 0) {
            // compare the address fields
            if ($employerAddress['values'][0]['street_address'] != $dao->street_address) $createNewAddress = TRUE;
            if ($employerAddress['values'][0]['supplemental_address_1'] != $dao->supplemental_address_1) $createNewAddress = TRUE;
            if ($employerAddress['values'][0]['city'] != $dao->city) $createNewAddress = TRUE;
            if ($employerAddress['values'][0]['postal_code'] != $dao->postal_code) $createNewAddress = TRUE;
            if ($employerAddress['values'][0]['country_id'] != $dao->country_id) $createNewAddress = TRUE;
          }
          else {
            // employer has no address
            $createNewAddress = TRUE;
          }

          if ($createNewAddress) {
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
          }
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

  public function importDGs() {
    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      $sql = "select * from tmpbeuc_dgs";
      $dao = CRM_Core_DAO::executeQuery($sql);

      // fill the queue with group id's
      while ($dao->fetch()) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importDGTask'], [$dao->external_identifier]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import DGs');
    }

    return $msg;
  }

  public function importEPcomms() {
    $msg = '';

    // create queue
    $this->createQueue();

    if ($this->queue->numberOfItems() > 0) {
      $msg = 'The queue is not empty: it contains ' . $this->queue->numberOfItems() . ' item(s).';
    }
    else {
      $sql = "select * from tmpbeuc_ep_committees where contact_sub_type is not null";
      $dao = CRM_Core_DAO::executeQuery($sql);

      // fill the queue with group id's
      while ($dao->fetch()) {
        $task = new CRM_Queue_Task(['CRM_Beucimport_Helper', 'importEPcommsTask'], [$dao->gid]);
        $this->queue->createItem($task);
      }

      $msg = 'Running queue';
      $this->runQueue('Import EP Committees');
    }

    return $msg;
  }

  public static function importEPcommsTask(CRM_Queue_TaskContext $ctx, $gid) {
    // select the committee
    $sql = "select * from tmpbeuc_ep_committees where gid = $gid";
    $dao = CRM_Core_DAO::executeQuery($sql);
    $dao->fetch();

    // check if we have the contact
    $params = [
      'external_identifier' => $dao->external_identifier,
      'sequential' => 1,
    ];
    $committee = civicrm_api3('Contact', 'get', $params);
    if ($committee['count'] == 0) {
      // create the EP committee
      $params['organization_name'] = $dao->organization_name;
      $params['contact_type'] = 'Organization';
      $params['contact_sub_type'] = str_replace(' ', '_', $dao->contact_sub_type);
      $committee = civicrm_api3('Contact', 'create', $params);
    }
    $committeeID = $committee['values'][0]['id'];

    // lookup the groups that have to be copied to this group
    $groupIDs = CRM_Core_DAO::singleValueQuery("select group_concat(gid) from tmpbeuc_ep_committees where copy_contacts_to = $gid");
    if ($groupIDs) {
      $groupIDs = $gid . ',' . $groupIDs;
    }
    else {
      $groupIDs = $gid;
    }

    // select all contacts in this group
    $sql = "select * from tmpbeuc_ep_contacts where gid in ($groupIDs)";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      // lookup the contact
      $contact = civicrm_api3('Contact', 'get', [
        'external_identifier' => 'uid_' . $dao->uid,
        'sequential' => 1
      ]);
      if ($contact['count'] == 0) {
        watchdog('beuc', 'uid_' . $dao->uid . ' not found');
      }
      else {
        // select the rel. type id
        $sql = "select id from civicrm_relationship_type where name_a_b = %1";

        if ($dao->default_relationship) {
          $relName = $dao->default_relationship;
        }
        else {
          $relName = $dao->relationship_type;
        }

        $sqlParams = [
          1 => [$relName, 'String'],
        ];
        $relTypeID = CRM_Core_DAO::singleValueQuery($sql, $sqlParams);

        if (!$relTypeID) {
          throw new Exception("relationship type not found: $relName");
        }

        try {
          // create the relationship
          $params = [
            'contact_id_a' => $contact['values'][0]['id'],
            'contact_id_b' => $committeeID,
            'relationship_type_id' => $relTypeID,
            'is_active' => 1,

          ];
          civicrm_api3('Relationship', 'create', $params);
        }
        catch (CiviCRM_API3_Exception $e) {
          if ($e->getMessage() == 'Invalid Relationship') {
            // add to group to check manually
            civicrm_api3('GroupContact', 'create', [
              'contact_id' => $contact['values'][0]['id'],
              'group_id' => 'ec_check',
            ]);
          }
          elseif ($e->getMessage() == 'Duplicate Relationship') {
            // ignore
          }
          else {
            watchdog('beuc', $contact['values'][0]['id'] . ' - ' . $committeeID . ' | ' . $e->getMessage());
          }
        }
      }
    }

    return TRUE;
  }

  public static function importDGTask(CRM_Queue_TaskContext $ctx, $ext_id) {
    $workingAt = CRM_Core_DAO::singleValueQuery("select id from civicrm_relationship_type where name_a_b = 'Working at'");

    // select the name of the group
    $sql = "
      select
        dg.*
        , c.id contact_id
      from
        tmpbeuc_dgs dg
      left outer join
        civicrm_contact c on c.external_identifier = dg.external_identifier
      where
        dg.external_identifier = '$ext_id'
    ";
    $dgDao = CRM_Core_DAO::executeQuery($sql);
    $dgDao->fetch();

    // see if we have the dg
    if ($dgDao->contact_id) {
      // yes, store the id
      $dgID = $dgDao->contact_id;
    }
    else {
      // no, create the org
      $params = [
        'organization_name' => substr($dgDao->organization_name, 0, 128),
        'contact_type' => 'Organization',
        'contact_sub_type' => str_replace(' ', '_', $dgDao->contact_sub_type),
        'external_identifier' => $dgDao->external_identifier,
        'sequential' => 1,
      ];
      $result = civicrm_api3('contact', 'create', $params);
      $dgID = $result['values'][0]['id'];
    }

    // select the dg contacts
    $sql = "select * from tmpbeuc_dg_contacts where ext_dg_id = '$ext_id'";
    $dao = CRM_Core_DAO::executeQuery($sql);
    while ($dao->fetch()) {
      // lookup the contact
      $contact = civicrm_api3('Contact', 'get', ['external_identifier' => $dao->ext_person_id, 'sequential' => 1]);
      if ($contact['count'] == 0) {
        watchdog('beuc', $dao->ext_person_id . ' not found');
      }
      else {
        // select the rel. type id
        $sql = "select id from civicrm_relationship_type where name_a_b = %1";
        $sqlParams = [
          1 => [str_replace('Acting ', '', $contact['values'][0]['job_title']), 'String'],
        ];
        $relTypeID = CRM_Core_DAO::singleValueQuery($sql, $sqlParams);

        if (!$relTypeID) {
          $relTypeID = $workingAt;
        }

        try {
          // create the relationship
          $params = [
            'contact_id_a' => $contact['values'][0]['id'],
            'contact_id_b' => $dgID,
            'relationship_type_id' => $relTypeID,
            'is_active' => 1,

          ];
          civicrm_api3('Relationship', 'create', $params);
        }
        catch (CiviCRM_API3_Exception $e) {
          if ($e->getMessage() == 'Invalid Relationship') {
            // add to group to check manually
            civicrm_api3('GroupContact', 'create', [
              'contact_id' => $contact['values'][0]['id'],
              'group_id' => 'ec_check',
            ]);
          }
          elseif ($e->getMessage() == 'Duplicate Relationship') {
            // ignore
          }
          else {
            watchdog('beuc', $contact['values'][0]['id'] . ' - ' . $dgID . ' | ' . $e->getMessage());
          }
        }
      }
    }

    return TRUE;
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