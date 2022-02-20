Select
    Id,
    Name,
    PersonEmail,
    area__c
FROM
    Account
WHERE
    IsDeleted = False
