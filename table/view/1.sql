CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `mydb`.`view1` AS
    SELECT 
        `mydb`.`protocol_info`.`publishDate` AS `p_Date`,
        `mydb`.`protocol_info`.`purchaseNumber` AS `№`,
        `mydb`.`NotificationEA44`.`purchaseObjectInfo` AS `obj_name`,
        `mydb`.`NotificationEA44`.`maxPrice` AS `maxPrice`,
        `mydb`.`NotificationEA44`.`contractGuarantee` AS `cont_guar`,
        `mydb`.`publishorg`.`fullName` AS `p_name`,
        `mydb`.`publishorg`.`INN` AS `p_inn`,
        `mydb`.`protocol`.`appRating` AS `rating`,
        `mydb`.`protocol`.`price` AS `price`,
        `mydb`.`ooo`.`INN` AS `org_inn`,
        `mydb`.`ooo`.`fullName` AS `org_name`
    FROM
        ((((`mydb`.`protocol_info`
        LEFT JOIN `mydb`.`NotificationEA44` ON ((`mydb`.`protocol_info`.`purchaseNumber` = `mydb`.`NotificationEA44`.`purchaseNumber`)))
        LEFT JOIN `mydb`.`publishorg` ON ((`mydb`.`protocol_info`.`regNum` = `mydb`.`publishorg`.`regNum`)))
        LEFT JOIN `mydb`.`protocol` ON ((`mydb`.`protocol`.`id` = `mydb`.`protocol_info`.`id`)))
        LEFT JOIN `mydb`.`ooo` ON ((`mydb`.`protocol`.`INN` = `mydb`.`ooo`.`INN`)))