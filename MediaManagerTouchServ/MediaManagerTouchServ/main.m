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
        }) version: @"0.0.1"];
        
        [design defineViewNamed: @"by_creation_time" mapBlock: MAPBLOCK({
            if ([[doc objectForKey: @"class_name"]
                 isEqualToString:@"plm.Image"]) {
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
                //     <original image part> ::= doc.oid,0,doc.size.width
                //     <derived image part>  ::= doc.orig_id,1,doc.size.width
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
                NSNumber* docWidth = [[doc objectForKey: @"size"] objectForKey: @"width"];
                        
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    isDerived = [NSNumber numberWithInteger: 0];
                }
                else {
                    oid = doc[@"orig_id"];
                    isDerived = [NSNumber numberWithInteger: 1];
                }
                        
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
                      isDerived,
                      docWidth,
                      nil],
                     docPath);
            }
        }) version: @"0.0.5"];
        
        [design defineViewNamed: @"batch_by_ctime" mapBlock: MAPBLOCK({
            if ([[doc objectForKey: @"class_name"]
                 isEqualToString:@"plm.ImportBatch"]) {
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

                id docPath = doc[@"path"];
                emit([NSArray arrayWithObjects:
                      year,
                      month,
                      day,
                      hour,
                      minute,
                      second,
                      milliseconds,
                      nil],
                     docPath);
            }
        }) version: @"0.0.0"];
        
        [design defineViewNamed: @"batch_by_oid_w_image" mapBlock: MAPBLOCK({
            if ([[doc objectForKey: @"class_name"]
                 isEqualToString:@"plm.ImportBatch"]) {
                //
                //  emit([doc.oid, '0', 0, 0], doc.path)
                //
                NSString* oid = doc[@"oid"];
                id docPath = doc[@"path"];
                emit([NSArray arrayWithObjects:
                      oid,
                      @"0",
                      [NSNumber numberWithInteger: 0],
                      [NSNumber numberWithInteger: 0],
                      nil],
                     docPath);
            }
            else if ([[doc objectForKey: @"class_name"]
                     isEqualToString:@"plm.Import"]) {
                //
                //  original doc:
                //    emit([doc.batch_id, doc.oid, 1, doc.size.width], doc.path)
                //
                //  variant:
                //    emit([doc.batch_id, doc.orig_id, 2, doc.size.width], doc.path)
                //
                NSString* batchId = doc[@"batch_id"];
                NSString* oid;
                NSNumber* originalFlag;
                NSNumber* docWidth = [[doc objectForKey: @"size"] objectForKey: @"width"];
                
                if ([[doc objectForKey: @"orig_id"] isEqualToString:@""]) {
                    oid = doc[@"oid"];
                    originalFlag = [NSNumber numberWithInteger: 1];
                }
                else {
                    oid = doc[@"orig_id"];
                    originalFlag = [NSNumber numberWithInteger: 2];
                }
                id docPath = doc[@"path"];
                emit([NSArray arrayWithObjects:
                      batchId,
                      oid,
                      originalFlag,
                      docWidth,
                      nil],
                     docPath);
            }
        }) version: @"0.0.0"];
        
        [design saveChanges];
                
        // Start a listener socket:
        [server tellTDServer: ^(TD_Server* tdServer) {
            TDListener* listener = [[TDListener alloc] initWithTDServer: tdServer port: kPortNumber];
            listener.readOnly = options.readOnly;
            [listener start];
        }];
        NSLog(@"MediaManagerTouchServ %@ is listening%@ on port %d ... relax!",
              [TDRouter versionString],
              (options.readOnly ? @" in read-only mode" : @""),
              kPortNumber);
                
        [[NSRunLoop currentRunLoop] run];
                
        NSLog(@"MediaManagerTouchServ quitting");
        
    }
    return 0;
}

