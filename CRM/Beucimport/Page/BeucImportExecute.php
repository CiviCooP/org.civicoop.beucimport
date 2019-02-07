<?php
use CRM_Beucimport_ExtensionUtil as E;

class CRM_Beucimport_Page_BeucImportExecute extends CRM_Core_Page {

  public function run() {
    CRM_Utils_System::setTitle(E::ts('Beuc Import - Execute'));

    $msg = '';
    $action = CRM_Utils_Array::value('action', $_GET);

    $importHelper = new CRM_Beucimport_Helper();

    try {
      if ($action == 'createConfig') {
        $msg = $importHelper->createConfiguration();
      }
      elseif ($action == 'importOrganizations') {
        $msg = $importHelper->importOrganizations();
      }
      elseif ($action == 'importPersons') {
        $msg = $importHelper->importPersons();
      }
      elseif ($action == 'deleteQueue') {
        $msg = $importHelper->deleteQueue();
      }
      elseif ($action == 'importGroups') {
        $msg = $importHelper->importGroups();
      }
      else {
        $msg = 'Error: Unknown action';
      }
    }
    catch (Exception $e) {
      $msg = 'Error: ' . $e->getMessage();
    }

    // process message (can be array or not)
    if (is_array($msg)) {
      if (count($msg) == 0) {
        $msg = '<p></p>';
      }
      elseif (count($msg) == 1) {
        $msg = '<p>' . $msg[0] . '</p>';
      }
      else {
        $msg = '<ul><li>' . implode('</li><li>', $msg) . '</li></ul>';
      }
    }
    else {
      $msg = '<p>' . $msg . '</p>';
    }

    $this->assign('action', $action);
    $this->assign('msg', $msg);

    parent::run();
  }

}
