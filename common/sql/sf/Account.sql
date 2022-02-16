Select
    Id,
    Name,
    PersonEmail,
    land__c,
    area__c,
    Area__pc,
    BuildingBudget__pc,
    KeiyakuBi1__c
FROM
    Account
WHERE
    IsDeleted = False
