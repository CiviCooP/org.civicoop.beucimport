<?php
use CRM_Beucimport_ExtensionUtil as E;

class CRM_Beucimport_Page_BeucImport extends CRM_Core_Page {

  /**
   * This is the import main page.
   * The corresponding .tpl file contains the menu.
   */
  public function run() {
    CRM_Utils_System::setTitle(E::ts('Beuk en Berk Import'));

    parent::run();
  }

}
