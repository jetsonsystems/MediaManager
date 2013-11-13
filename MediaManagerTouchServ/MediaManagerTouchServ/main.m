//
//  main.m
//  MediaManagerTouchServ
//
//  Created by Marek Ryniejski on 12/5/12.
//  Copyright (c) 2012 Marek Ryniejski. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <TouchDB/TD_DatabaseManager.h>
#import <TouchDB/TDRouter.h>
#import <TouchDBListener/TDListener.h>
#import <CouchCocoa/CouchCocoa.h>

#define kPortNumber 59840

static NSString* GetServerPath() {
    NSString* bundleID = [[NSBundle mainBundle] bundleIdentifier];
    if (!bundleID)
        bundleID = @"com.jetsonsystems.MediaManagerTouchServ";
    
    NSArray* paths = NSSearchPathForDirectoriesInDomains(
        NSApplicationSupportDirectory,
        NSUserDomainMask, YES);
    NSString* path = paths[0];
    path = [path stringByAppendingPathComponent: bundleID];
    path = [path stringByAppendingPathComponent: @"MediaManagerTouchDB"];
    NSError* error = nil;
    if (![[NSFileManager defaultManager] createDirectoryAtPath: path
        withIntermediateDirectories: YES
        attributes: nil error: &error]) {
        NSLog(@"FATAL: Couldn't create TouchDB server dir at %@", path);
        exit(1);
    }
    return path;
}

//
// main: Main program.
//
int main(int argc, const char * argv[])
{

    @autoreleasepool {

        TD_DatabaseManagerOptions options = kTD_DatabaseManagerDefaultOptions;
        options.readOnly = NO;
        const char* replArg = NULL, *user = NULL, *password = NULL;
        BOOL auth = NO, pull = NO, createTarget = NO, continuous = NO;
        NSString* dbName = @"plm-media-manager";
                
        for (int i = 1; i < argc; ++i) {
            if (strcmp(argv[i], "--readonly") == 0) {
                options.readOnly = YES;
            } else if (strcmp(argv[i], "--auth") == 0) {
                auth = YES;
            } else if (strcmp(argv[i], "--pull") == 0) {
                replArg = argv[++i];
                pull = YES;
            } else if (strcmp(argv[i], "--push") == 0) {
                replArg = argv[++i];
            } else if (strcmp(argv[i], "--create-target") == 0) {
                createTarget = YES;
            } else if (strcmp(argv[i], "--continuous") == 0) {
                continuous = YES;
            } else if (strcmp(argv[i], "--user") == 0) {
                user = argv[++i];
            } else if (strcmp(argv[i], "--password") == 0) {
                password = argv[++i];
            } else if (strcmp(argv[i], "--db") == 0) {
                dbName = [[NSString alloc] initWithUTF8String:argv[++i]];
            }
        }
        
        CouchTouchDBServer* server = [[CouchTouchDBServer alloc]
                                      initWithServerPath: GetServerPath()];

        NSLog(@"MediaManagerTouchServ creating database: %@!",
              dbName);

        CouchDatabase* database = [server databaseNamed: dbName];
                
        RESTOperation* op = [database create];
        if (![op wait]) {
            NSCAssert(op.error.code == 412, @"Error creating db!");
        }
        
        NSLog(@"MediaManagerTouchServ created database: %@!",
              dbName);
                
        // Setup up our design document.
        CouchDesignDocument* design =
            [database designDocumentWithName: @"_design/plm-image"];
        
        [design defineViewNamed: @"by_oid_with_variant" mapBlock: MAPBLOCK({
            if ([doc[@"class_name"] isEqualToString:@"plm.Image"]) {
                NSString* oid;
                NSNumber* isDerived;
                NSNumber* docWidth = [[doc objectForKey: @"size"] objectForKey: @"width"];
            
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    isDerived = [NSNumber numberWithInteger: 0];
                }
                else {
                    oid = doc[@"orig_id"];
                    isDerived = [NSNumber numberWithInteger: 1];
                }
                emit([NSArray arrayWithObjects:oid, isDerived, docWidth, nil],
                     doc[@"path"]);
            }
        }) version: @"0.0.2"];
        
        [design defineViewNamed: @"by_oid_without_variant" mapBlock: MAPBLOCK({
            if ([doc[@"class_name"] isEqualToString:@"plm.Image"]) {
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    emit(doc[@"oid"], doc[@"path"]);
                }
            }
        }) version: @"0.0.3"];
        
        [design defineViewNamed: @"by_creation_time" mapBlock: MAPBLOCK({
            if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) &&
                (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                id docCreatedAt = doc[@"created_at"];
                //
                // build the key:
                //
                //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                //
                //   key format: <key> ::= [<date part>,<image part>]
                //
                //     <date part> ::= <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                //
                //     <image part> ::= <original image part> | <derived image part>
                //     <original image part> ::= doc.name,doc.oid,0
                //     <derived image part>  ::= doc.name,doc.orig_id,1
                //
                NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                NSDate *date = [dateFormat dateFromString:docCreatedAt];
                NSCalendar* calendar = [NSCalendar currentCalendar];
                NSDateComponents* components = [calendar
                                                components:
                                                (NSYearCalendarUnit|
                                                 NSMonthCalendarUnit|
                                                 NSDayCalendarUnit|
                                                 NSHourCalendarUnit|
                                                 NSMinuteCalendarUnit|
                                                 NSSecondCalendarUnit) fromDate:date];
                NSNumber* year = [NSNumber numberWithInteger:[components year]];
                NSNumber* month = [NSNumber numberWithInteger:[components month]];
                NSNumber* day = [NSNumber numberWithInteger: [components day]];
                NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                NSNumber* second = [NSNumber numberWithInteger: [components second]];
                NSNumber* milliseconds = [NSNumber numberWithInteger: 0];
                        
                NSString* oid;
                NSNumber* isDerived;
                        
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    isDerived = [NSNumber numberWithInteger: 0];
                }
                else {
                    oid = doc[@"orig_id"];
                    isDerived = [NSNumber numberWithInteger: 1];
                }
                        
                id docName = doc[@"name"];
                emit([NSArray arrayWithObjects:
                      year,
                      month,
                      day,
                      hour,
                      minute,
                      second,
                      milliseconds,
                      oid,
                      isDerived,
                      docName,
                      nil],
                     docName);
            }
        }) version: @"0.0.11"];

        [design defineViewNamed: @"by_creation_time_tagged" mapBlock: MAPBLOCK({
            if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) &&
                (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                id docCreatedAt = doc[@"created_at"];
                //
                // build the key:
                //
                //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                //
                //   key format: <key> ::= [<date part>,<image part>]
                //
                //     <date part> ::= <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                //
                //     <image part> ::= <original image part> | <derived image part>
                //     <original image part> ::= doc.oid,0,doc.name
                //     <derived image part>  ::= doc.orig_id,1,doc.name
                //
                NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                NSDate *date = [dateFormat dateFromString:docCreatedAt];
                NSCalendar* calendar = [NSCalendar currentCalendar];
                NSDateComponents* components = [calendar
                                                components:
                                                (NSYearCalendarUnit|
                                                 NSMonthCalendarUnit|
                                                 NSDayCalendarUnit|
                                                 NSHourCalendarUnit|
                                                 NSMinuteCalendarUnit|
                                                 NSSecondCalendarUnit) fromDate:date];
                NSNumber* year = [NSNumber numberWithInteger:[components year]];
                NSNumber* month = [NSNumber numberWithInteger:[components month]];
                NSNumber* day = [NSNumber numberWithInteger: [components day]];
                NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                NSNumber* second = [NSNumber numberWithInteger: [components second]];
                NSNumber* milliseconds = [NSNumber numberWithInteger: 0];
                        
                NSString* oid;
                NSNumber* isDerived;
                id docName = doc[@"name"];

                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                  if ([doc objectForKey: @"tags"] && ([[doc objectForKey: @"tags"] count] > 0)) {
                        oid = doc[@"oid"];
                        isDerived = [NSNumber numberWithInteger: 0];
                        emit([NSArray arrayWithObjects:
                              year,
                              month,
                              day,
                              hour,
                              minute,
                              second,
                              milliseconds,
                              oid,
                              isDerived,
                              docName,
                              nil],
                              docName);
                  }
                }
                else {
                    oid = doc[@"orig_id"];
                    isDerived = [NSNumber numberWithInteger: 1];
                    emit([NSArray arrayWithObjects:
                          year,
                          month,
                          day,
                          hour,
                          minute,
                          second,
                          milliseconds,
                          oid,
                          isDerived,
                          docName,
                          nil],
                         docName);
                }
            }
        }) version: @"0.0.3"];

        [design defineViewNamed: @"by_creation_time_untagged" mapBlock: MAPBLOCK({
            if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) &&
                (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                id docCreatedAt = doc[@"created_at"];
                //
                // build the key:
                //
                //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                //
                //   key format: <key> ::= [<date part>,<image part>]
                //
                //     <date part> ::= <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                //
                //     <image part> ::= <original image part> | <derived image part>
                //     <original image part> ::= doc.name,doc.oid,0
                //     <derived image part>  ::= doc.name,doc.orig_id,1
                //
                NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                NSDate *date = [dateFormat dateFromString:docCreatedAt];
                NSCalendar* calendar = [NSCalendar currentCalendar];
                NSDateComponents* components = [calendar
                                                components:
                                                (NSYearCalendarUnit|
                                                 NSMonthCalendarUnit|
                                                 NSDayCalendarUnit|
                                                 NSHourCalendarUnit|
                                                 NSMinuteCalendarUnit|
                                                 NSSecondCalendarUnit) fromDate:date];
                NSNumber* year = [NSNumber numberWithInteger:[components year]];
                NSNumber* month = [NSNumber numberWithInteger:[components month]];
                NSNumber* day = [NSNumber numberWithInteger: [components day]];
                NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                NSNumber* second = [NSNumber numberWithInteger: [components second]];
                NSNumber* milliseconds = [NSNumber numberWithInteger: 0];
                        
                NSString* oid;
                NSNumber* isDerived;
                id docName = doc[@"name"];
                        
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    if (![doc objectForKey: @"tags"] || ([[doc objectForKey: @"tags"] count] <= 0)) {
                        isDerived = [NSNumber numberWithInteger: 0];
                        emit([NSArray arrayWithObjects:
                              year,
                              month,
                              day,
                              hour,
                              minute,
                              second,
                              milliseconds,
                              oid,
                              isDerived,
                              docName,
                              nil],
                             docName);
                    }
                }
                else {
                    oid = doc[@"orig_id"];
                    isDerived = [NSNumber numberWithInteger: 1];
                    emit([NSArray arrayWithObjects:
                          year,
                          month,
                          day,
                          hour,
                          minute,
                          second,
                          milliseconds,
                          oid,
                          isDerived,
                          docName,
                          nil],
                         docName);
                }
            }
        }) version: @"0.0.3"];

        [design defineViewNamed: @"by_creation_time_name" 
                       mapBlock: MAPBLOCK({
                               if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) &&
                                   (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                                   id docCreatedAt = doc[@"created_at"];
                                   //
                                   // build the key (originals ONLY):
                                   //
                                   //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                                   //
                                   //   key format: <key> ::= [<date part>,<image part>]
                                   //
                                   //     <date part> ::= <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                                   //
                                   //     <image part> ::= doc.name,doc.oid
                                   //
                                   NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                                   [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                                   NSDate *date = [dateFormat dateFromString:docCreatedAt];
                                   NSCalendar* calendar = [NSCalendar currentCalendar];
                                   NSDateComponents* components = [calendar components:
                                                                                (NSYearCalendarUnit|
                                                                                 NSMonthCalendarUnit|
                                                                                 NSDayCalendarUnit|
                                                                                 NSHourCalendarUnit|
                                                                                 NSMinuteCalendarUnit|
                                                                                 NSSecondCalendarUnit) fromDate:date];
                                   NSNumber* year = [NSNumber numberWithInteger:[components year]];
                                   NSNumber* month = [NSNumber numberWithInteger:[components month]];
                                   NSNumber* day = [NSNumber numberWithInteger: [components day]];
                                   NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                                   NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                                   NSNumber* second = [NSNumber numberWithInteger: [components second]];
                                   NSNumber* milliseconds = [NSNumber numberWithInteger: 0];
                        
                                   NSString* oid;
                        
                                   if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                                       oid = doc[@"oid"];
                                       id docName = doc[@"name"];
                                       emit([NSArray arrayWithObjects:
                                                         year,
                                                         month,
                                                         day,
                                                         hour,
                                                         minute,
                                                         second,
                                                         milliseconds,
                                                         docName,
                                                         oid,
                                                         nil],
                                            docName);
                                   }
                               }
                           })
                    reduceBlock: ^(NSArray* keys, NSArray* values, BOOL rereduce) {
                        if (rereduce) {
                            NSNumber* ni = [NSNumber numberWithInteger: 0];
                            for (NSNumber* value in values) {
                                ni = [NSNumber numberWithInteger: [ni intValue] + [value intValue]];
                            }
                            return ni;
                        }
                        else {
                            return [NSNumber numberWithInteger: values.count];
                        }
                    }
                    version: @"0.0.0"];

        [design defineViewNamed: @"by_creation_time_name_tagged" 
                       mapBlock: MAPBLOCK({
                               if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) &&
                                   (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                                   id docCreatedAt = doc[@"created_at"];
                                   //
                                   // build the key (original images only):
                                   //
                                   //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                                   //
                                   //   key format: <key> ::= [<date part>,<image part>]
                                   //
                                   //     <date part> ::= <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                                   //
                                   //     <image part> ::= <original image part>
                                   //     <original image part> ::= doc.name, doc.oid
                                   //
                                   NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                                   [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                                   NSDate *date = [dateFormat dateFromString:docCreatedAt];
                                   NSCalendar* calendar = [NSCalendar currentCalendar];
                                   NSDateComponents* components = [calendar
                                                                      components:
                                                                          (NSYearCalendarUnit|
                                                                           NSMonthCalendarUnit|
                                                                           NSDayCalendarUnit|
                                                                           NSHourCalendarUnit|
                                                                           NSMinuteCalendarUnit|
                                                                           NSSecondCalendarUnit) fromDate:date];
                                   NSNumber* year = [NSNumber numberWithInteger:[components year]];
                                   NSNumber* month = [NSNumber numberWithInteger:[components month]];
                                   NSNumber* day = [NSNumber numberWithInteger: [components day]];
                                   NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                                   NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                                   NSNumber* second = [NSNumber numberWithInteger: [components second]];
                                   NSNumber* milliseconds = [NSNumber numberWithInteger: 0];
                                   
                                   NSString* oid;
                                   id docName = doc[@"name"];

                                   if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                                       if ([doc objectForKey: @"tags"] && ([[doc objectForKey: @"tags"] count] > 0)) {
                                           oid = doc[@"oid"];
                                           emit([NSArray arrayWithObjects:
                                                         year,
                                                         month,
                                                         day,
                                                         hour,
                                                         minute,
                                                         second,
                                                         milliseconds,
                                                         docName,
                                                         oid,
                                                         nil],
                                                docName);
                                       }
                                   }
                               }
                           })
                    reduceBlock: ^(NSArray* keys, NSArray* values, BOOL rereduce) {
                        if (rereduce) {
                            NSNumber* ni = [NSNumber numberWithInteger: 0];
                            for (NSNumber* value in values) {
                                ni = [NSNumber numberWithInteger: [ni intValue] + [value intValue]];
                            }
                            return ni;
                        }
                        else {
                            return [NSNumber numberWithInteger: values.count];
                        }
                    }
        version: @"0.0.0"];

        [design defineViewNamed: @"by_creation_time_name_untagged" mapBlock: MAPBLOCK({
            if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) &&
                (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                id docCreatedAt = doc[@"created_at"];
                //
                // build the key:
                //
                //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                //
                //   key format: <key> ::= [<date part>,<image part>]
                //
                //     <date part> ::= <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                //
                //     <image part> ::= <original image part> | <derived image part>
                //     <original image part> ::= doc.name,doc.oid,0
                //     <derived image part>  ::= doc.name,doc.orig_id,1
                //
                NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                NSDate *date = [dateFormat dateFromString:docCreatedAt];
                NSCalendar* calendar = [NSCalendar currentCalendar];
                NSDateComponents* components = [calendar
                                                components:
                                                (NSYearCalendarUnit|
                                                 NSMonthCalendarUnit|
                                                 NSDayCalendarUnit|
                                                 NSHourCalendarUnit|
                                                 NSMinuteCalendarUnit|
                                                 NSSecondCalendarUnit) fromDate:date];
                NSNumber* year = [NSNumber numberWithInteger:[components year]];
                NSNumber* month = [NSNumber numberWithInteger:[components month]];
                NSNumber* day = [NSNumber numberWithInteger: [components day]];
                NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                NSNumber* second = [NSNumber numberWithInteger: [components second]];
                NSNumber* milliseconds = [NSNumber numberWithInteger: 0];
                        
                NSString* oid;
                NSNumber* isDerived;
                id docName = doc[@"name"];
                        
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    if (![doc objectForKey: @"tags"] || ([[doc objectForKey: @"tags"] count] <= 0)) {
                        isDerived = [NSNumber numberWithInteger: 0];
                        emit([NSArray arrayWithObjects:
                              year,
                              month,
                              day,
                              hour,
                              minute,
                              second,
                              milliseconds,
                              docName,
                              oid,
                              nil],
                             docName);
                    }
                }
            }
        })
                    reduceBlock: ^(NSArray* keys, NSArray* values, BOOL rereduce) {
                        if (rereduce) {
                            NSNumber* ni = [NSNumber numberWithInteger: 0];
                            for (NSNumber* value in values) {
                                ni = [NSNumber numberWithInteger: [ni intValue] + [value intValue]];
                            }
                            return ni;
                        }
                        else {
                            return [NSNumber numberWithInteger: values.count];
                        }
                    }
        version: @"0.0.0"];
        
        [design defineViewNamed: @"batch_by_ctime" mapBlock: MAPBLOCK({
            if (([[doc objectForKey: @"class_name"] isEqualToString:@"plm.ImportBatch"]) &&
                (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue])) {
                id docCreatedAt = doc[@"created_at"];
                //
                // build the key:
                //
                //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                //
                //   key format: <key> ::= [<date part>]
                //
                //     <date part> ::=
                //       <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                //
                NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                NSDate *date = [dateFormat dateFromString:docCreatedAt];
                NSCalendar* calendar = [NSCalendar currentCalendar];
                NSDateComponents* components = [calendar
                                                components:
                                                (NSYearCalendarUnit|
                                                 NSMonthCalendarUnit|
                                                 NSDayCalendarUnit|
                                                 NSHourCalendarUnit|
                                                 NSMinuteCalendarUnit|
                                                 NSSecondCalendarUnit) fromDate:date];
                NSNumber* year = [NSNumber numberWithInteger:[components year]];
                NSNumber* month = [NSNumber numberWithInteger:[components month]];
                NSNumber* day = [NSNumber numberWithInteger: [components day]];
                NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                NSNumber* second = [NSNumber numberWithInteger: [components second]];
                NSNumber* milliseconds = [NSNumber numberWithInteger: 0];

                NSString* oid = doc[@"oid"];
                id docPath = doc[@"path"];
                emit([NSArray arrayWithObjects:
                      year,
                      month,
                      day,
                      hour,
                      minute,
                      second,
                      milliseconds,
                      oid,
                      nil],
                     docPath);
            }
        }) version: @"0.0.1"];

        //
        // batch_by_oid_w_image:
        //
        //  key: <batch_id>, <original image id>, <0, 1, 2 depending upon whether import, original, or variant>, <name>
        //        
        [design defineViewNamed: @"batch_by_oid_w_image" mapBlock: MAPBLOCK({
            if ([[doc objectForKey: @"class_name"] isEqualToString:@"plm.ImportBatch"]) {
                //
                //  emit([doc.oid, '0', 0, ''], doc.path)
                //
                NSString* oid = doc[@"oid"];
                id docPath = doc[@"path"];
                emit([NSArray arrayWithObjects:
                      oid,
                      @"0",
                      [NSNumber numberWithInteger: 0],
                      @"",
                      nil],
                     docPath);
            }
            else if ([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) {
                //
                //  original doc:
                //    emit([doc.batch_id, doc.oid, 1, doc.name], doc.name)
                //
                //  variant:
                //    emit([doc.batch_id, doc.orig_id, 2, doc.name], doc.name)
                //
                NSString* batchId = doc[@"batch_id"];
                NSString* oid;
                NSNumber* originalFlag;
                id docName = doc[@"name"];
                
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    originalFlag = [NSNumber numberWithInteger: 1];
                }
                else {
                    oid = doc[@"orig_id"];
                    originalFlag = [NSNumber numberWithInteger: 2];
                }

                emit([NSArray arrayWithObjects:
                      batchId,
                      oid,
                      originalFlag,
                      docName,
                      nil],
                     docName);
            }
        }) version: @"0.0.7"];

        //
        // batch_by_oid_w_image_by_ctime: 
        //
        //  key: <batch_id>, <0, 1, 2 depending upon whether import, original or variant>, <in trash>, <date>, <"" or image.name>, <'0' or original image id>
        //  value: <doc.path or doc.name>
        //  note: Date is 7 fields -> key length is 12.
        //
        [design defineViewNamed: @"batch_by_oid_w_image_by_ctime" 
                       mapBlock: MAPBLOCK({
                           if ([[doc objectForKey: @"class_name"] isEqualToString:@"plm.ImportBatch"]) {
                             //
                             // emit([doc.oid, 0, 0, 0, 0, 0, 0, 0, 0, '', '0'], doc.path)
                             //
                             NSString* oid = doc[@"oid"];
                             NSNumber* zero = [NSNumber numberWithInteger: 0];
                             id docPath = doc[@"path"];
                             emit([NSArray
                                    arrayWithObjects:
                                      oid,
                                      zero,
                                      zero,
                                      zero,
                                      zero,
                                      zero,
                                      zero,
                                      zero,
                                      zero,
                                      @"",
                                      @"0",
                                      nil
                                   ],
                                             docPath);
                           }
                           else if ([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) {
                             //
                             // Original image:
                             //
                             //  emit([doc.batch_id, 1, doc.in_trash, <created_at>, doc.name, doc.oid], doc.name);
                             //
                             // Variant:
                             //
                             //  emit([doc.batch_id, 2, doc.in_trash, <ccreated_at>, doc.name, doc.orig_id], doc.name);
                             //
                             NSString* batchId = doc[@"batch_id"];
                             NSNumber* originalFlag;
                             NSNumber* trashFlag;
                             NSString* oid;

                             if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                               oid = doc[@"oid"];
                               originalFlag = [NSNumber numberWithInteger: 1];
                             }
                             else {
                               oid = doc[@"orig_id"];
                               originalFlag = [NSNumber numberWithInteger: 2];
                             }
                             if ([[doc objectForKey: @"in_trash"] boolValue]) {
                               trashFlag = [NSNumber numberWithInteger: 1];
                             }
                             else {
                               trashFlag = [NSNumber numberWithInteger: 0];
                             }

                             id docCreatedAt = doc[@"created_at"];
                             //
                             // build the key:
                             //
                             //   note created_at is in this format: 2012-12-05T01:37:55.087Z
                             //
                             //   key format: <key> ::= [<date part>]
                             //
                             //     <date part> ::=
                             //       <year>,<month>,<day>,<hours>,<minutes>,<seconds>,<milliseconds>
                             //
                             NSDateFormatter *dateFormat = [[NSDateFormatter alloc] init];
                             [dateFormat setDateFormat:@"YYYY'-'MM'-'dd'T'HH':'mm':'ss'.'SSS'Z'"];
                             NSDate *date = [dateFormat dateFromString:docCreatedAt];
                             NSCalendar* calendar = [NSCalendar currentCalendar];
                             NSDateComponents* components = [calendar
                                                              components:
                                                                (NSYearCalendarUnit|
                                                                 NSMonthCalendarUnit|
                                                                 NSDayCalendarUnit|
                                                                 NSHourCalendarUnit|
                                                                 NSMinuteCalendarUnit|
                                                                 NSSecondCalendarUnit) fromDate:date];
                             NSNumber* year = [NSNumber numberWithInteger:[components year]];
                             NSNumber* month = [NSNumber numberWithInteger:[components month]];
                             NSNumber* day = [NSNumber numberWithInteger: [components day]];
                             NSNumber* hour = [NSNumber numberWithInteger: [components hour]];
                             NSNumber* minute = [NSNumber numberWithInteger: [components minute]];
                             NSNumber* second = [NSNumber numberWithInteger: [components second]];
                             
                             id docName = doc[@"name"];
                             emit([NSArray arrayWithObjects:
                                             batchId,
                                             originalFlag,
                                             trashFlag,
                                             year,
                                             month,
                                             day,
                                             hour,
                                             minute,
                                             second,
                                             docName,
                                             oid,
                                             nil],
                                  docName);
                           }
                         })
                    reduceBlock: ^(NSArray* keys, NSArray* values, BOOL rereduce) {
                        //
                        //  function(keys, values, rereduce) {
                        //    var reduced = { num_images: 0, num_images_intrash: 0 }; 
                        //    if (rereduce) {
                        //      for (var i = 0; i < values.length; i++) {
                        //        var value = values[i]; 
                        //        reduced.num_images = reduced.num_images + value.num_images;
                        //        reduced.num_images_intrash = reduced.num_images_intrash + value.num_images_intrash;
                        //      }
                        //    }
                        //    else {
                        //      var ni = 0;
                        //      var nit = 0;
                        //      for (var i = 0; i < keys.length; i++) {
                        //        var key = keys[i][0];
                        //        if (key[1] === 1) {
                        //          ni++; 
                        //          if (key[2] === 1) {
                        //            nit++;
                        //          }
                        //        }
                        //      }
                        //      reduced.num_images = ni; 
                        //      reduced.num_images_intrash = nit;
                        //    }
                        //    return reduced;
                        //  }
                        //
                        NSMutableDictionary* reduced = [NSMutableDictionary dictionaryWithObjectsAndKeys: 
                                                                             [NSNumber numberWithInteger: 0], @"num_images", 
                                                                             [NSNumber numberWithInteger: 0], @"num_images_intrash", 
                                                                             nil];

                        if (rereduce) {
                          for (NSMutableDictionary* value in values) {
                            NSNumber* ni = [NSNumber numberWithInteger: [[reduced objectForKey: @"num_images"] intValue] + [[value objectForKey: @"num_images"] intValue]];
                            [reduced setObject: ni forKey:@"num_images"];
                            NSNumber* nit = [NSNumber numberWithInteger: [[reduced objectForKey: @"num_images_intrash"] intValue] + [[value objectForKey: @"num_images_intrash"] intValue]];
                            [reduced setObject: nit forKey:@"num_images_intrash"];
                          }
                        }
                        else {
                          NSNumber* ni = [NSNumber numberWithInteger: 0];
                          NSNumber* nit = [NSNumber numberWithInteger: 0];
                          for (NSArray* key in keys) {
                            if ([key[1] isEqualToNumber: [NSNumber numberWithInteger: 1]]) {
                              ni = [NSNumber numberWithInteger: [ni intValue] + 1];
                              if ([key[2] isEqualToNumber: [NSNumber numberWithInteger: 1]]) {
                                nit = [NSNumber numberWithInteger: [nit intValue] + 1];
                              }
                            }
                          }
                          [reduced setObject: ni forKey:@"num_images"];
                          [reduced setObject: nit forKey:@"num_images_intrash"];
                        }
                        return reduced;
                    }
                    version: @"0.0.2"];

        [design
         defineViewNamed: @"by_tag"
         mapBlock: MAPBLOCK({
            if ([[doc objectForKey: @"class_name"] isEqualToString:@"plm.Image"]) {
                if (![doc objectForKey: @"in_trash"] || ![[doc objectForKey: @"in_trash"] boolValue]) {
                    if ([doc objectForKey: @"tags"]) {
                        for (id tag in [doc objectForKey: @"tags"]) {
                            emit(tag, doc[@"tags"]);
                        }
                    }
                }
            }
        })
         reduceBlock: REDUCEBLOCK(return [NSNumber numberWithInteger:1];)
         version: @"0.0.3"
         ];

        [design defineViewNamed: @"by_trash" mapBlock: MAPBLOCK({
            if ([doc objectForKey: @"in_trash"]) {
                if ([[doc objectForKey: @"in_trash"] boolValue]) {
                    NSString* oid = doc[@"oid"];
                    id docPath = doc[@"path"];
                    emit(oid, docPath);
                }
            }
        }) version: @"0.0.3"];
        
        [design saveChanges];
                
        // Start a listener socket:
        [server tellTDServer: ^(TD_Server* tdServer) {
            TDListener* listener = [[TDListener alloc] initWithTDServer: tdServer port: kPortNumber];
            listener.readOnly = options.readOnly;
            [listener start];
        }];
        NSString* serverVersion = @"0.0.3";
        NSLog(@"MediaManagerTouchServ %@ is listening%@ on port %d ... relax!",
              serverVersion,
              (options.readOnly ? @" in read-only mode" : @""),
              kPortNumber);
                
        [[NSRunLoop currentRunLoop] run];
                
        NSLog(@"MediaManagerTouchServ quitting");
        
    }
    return 0;
}

