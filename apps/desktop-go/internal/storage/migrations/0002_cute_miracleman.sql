ALTER TABLE `messages` ADD `display_content` text;
ALTER TABLE `messages` ADD `attachments` text DEFAULT '[]' NOT NULL;
