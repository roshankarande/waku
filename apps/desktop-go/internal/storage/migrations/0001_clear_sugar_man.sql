ALTER TABLE `sessions` ADD `auto_title` text;
UPDATE `sessions` SET `auto_title` = CASE WHEN trim(`title`) <> '' AND `title` <> 'New task' THEN `title` ELSE NULL END, `title` = 'New task';
