<?php

use Oasis\Mlib\Logging\ConsoleHandler;
use Oasis\Mlib\Logging\LocalFileHandler;

require_once __DIR__ . "/../vendor/autoload.php";

(new LocalFileHandler())->install();
if (in_array("-v", $_SERVER['argv'])) {
    (new ConsoleHandler())->install();
}
