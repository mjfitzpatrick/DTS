/**
 *  DTSDB.C -- Mini interface to DTS database functions.
 *
 *  DB Table Schema:
 *
 *  Entry:
 *	key			primary key (unique) 
 *	queue			queue name
 *	src			name of src machine
 *	dst			name of dst machine
 *	hfname			host filename (original <node!/path/file.fits>))
 *	qpath			local queue path
 *	size			file size (bytes)
 *	md5			MD5 checksum
 *	nerrs			number of errors
 *	status			(-1=failed, 0=pending, 1=active, 2=complete)
 *	time_in			ingest time
 *	tstart			transfer start time
 *	tend			transfer end time
 *	time_out		delivery time
 *	status			(0=OK, 1=FAILED)
 *
 *  Message:
 *	key			primary key
 *	mtime			message timestamp
 *	msg			status or message
 *
 *
 *  Interface:
 *
 *	            dts_dbOpen  (dbfile)
 *	          dts_dbCreate  (dbfile)
 *                  dts_dbInit  ()
 *	           dts_dbClose  ()
 *
 *          res = dts_dbLookup  (fname, queue, *nres)
 *        res = dts_dbRawQuery  (sql, *nres)
 *		 dts_dbResFree  (res)
 *  							// Add or Del entries
 *	        dts_dbNewEntry  (queue, src, dst, hfname, qpath, size, md5)
 *	        dts_dbDelEntry  (key)
 *                 dts_dbPurge  (epoch)
 *  							// Set or Get values
 *	          dts_dbSetInt  (key, field, ival)
 *	          dts_dbSetStr  (key, field, str)
 *         ival = dts_dbGetInt  (key, field)
 *          str = dts_dbGetStr  (key, field)
 *
 *	         dts_dbSetTime  (key, field)
 *	 epoch = dts_dbGetTime  (key, field, local)
 *	 tstr = dts_dbGetEpoch  (key, field, local)
 *
 *	          dts_dbAddMsg  (key, msg)
 *       msg[] = dts_dbGetMsgs  (key)
 *
 *
 *
 *  @file       dtsdb.c
 *  @author     Mike Fitzpatrick
 *  @date       1/10/11
 *
 *  @brief  	Mini interface to DTS database functions.
 **/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sqlite3.h>
#include <sys/types.h>
#if defined(SYSV) || defined(MACOSX)
#include <time.h>
#else
#include <sys/time.h>
#include <sys/timeb.h>
#endif

#include "dtsdb.h"


#define	SZ_NAMEBUF		64


char *tkeys[] =  { "time_in", 			/* timestamp colum names     */
	 	   "time_out",
	 	   "tstart", 
	 	   "tend", 
	 	   "tpurged" };

char *mk_entry_tbl = \
    "CREATE TABLE IF NOT EXISTS entry ("
        "key       INTEGER PRIMARY KEY AUTOINCREMENT,"
        "queue     VARCHAR(15),"
        "src       VARCHAR(15),"
        "dst       VARCHAR(15),"
        "hfname    TEXT,"
        "qpath     TEXT,"
        "size      INTEGER,"
        "md5       TEXT,"
        "nerrs     INTEGER,"
        "time_in   DATE,"
        "time_out  DATE,"
        "tstart    DATE,"
        "tend      DATE,"
        "tpurged   DATE,"
        "ttime     REAL,"
        "mbs       REAL,"
        "status    INTEGER);";

char *mk_entry_trigger = \
    " "
    "CREATE TRIGGER update_mbs AFTER UPDATE OF tend ON entry "
    "  BEGIN "
    "    UPDATE entry SET ttime=((strftime('%f',tend)-strftime('%f',tstart)))  "
    "      WHERE key = new.key; "
    "    UPDATE entry SET mbs=( (((8*size)/1045876.0)/ttime))  "
    "      WHERE key = new.key; "
    "    UPDATE entry SET size=(size+1) WHERE key=1; "
    "  END; ";

char *mk_msg_tbl = \
    "CREATE TABLE IF NOT EXISTS messages "
	"(key INTEGER PRIMARY KEY, ekey INTEGER, mtime DATE, mstr TEXT);";

char *drop_entry_tbl = "DROP TABLE IF EXISTS entry;";

char *drop_msg_tbl   = "DROP TABLE IF EXISTS messages;";



                                        /* entry or message table fields    */
char *ke_fields[] = { "key",        "queue",        "src",
                      "dst",        "hfname",       "qpath",
                      "size",       "md5",          "nerrs",
                      "time_in",    "time_out",     "tstart",
                      "tend",       "tpurged",	    "ttime",
	              "mbs",        "status",	     NULL
                    };
char *km_fields[] = { "key",    "ekey",     "mtime",     "mstr",     NULL };



static int status 	= 0;		/* global status flag		    */
static int reskey 	= 0;		/* incremental result set key	    */

struct {
    sqlite3       *handle;
    sqlite3_stmt  *stmt;
    char           *dbfile;
    int            debug;
} db = {
    (sqlite3 *)      NULL,              /* handle       		    */
    (sqlite3_stmt *) NULL,              /* stmt         		    */
    (char *)         NULL,              /* dbfile       		    */
                        0               /* debug        		    */
};

pthread_mutex_t db_mut = PTHREAD_MUTEX_INITIALIZER;	/* mutex lock       */

static char *dts_dbInitResult (void);




/*****************************************************************************/

/** 
 *  DTS_DBOPEN -- Open the named database file and initialize the connection.
 *  The database will be created if it does not currently exist.
 *
 *  @brief	Open the named database file and initialize the connection.
 *  @fn		int dts_dbOpen (char *dbfile)
 *
 *  @param	dbfile		database file name
 *  @return			status code (OK or ERR)
 */
int
dts_dbOpen (char *dbfile)
{
    char  namebuf[SZ_NAMEBUF];
    int	  exists = (access (dbfile, F_OK) == 0);


    memset (namebuf, 0, SZ_NAMEBUF);
    gethostname (namebuf, SZ_NAMEBUF);

    /*  Open the named database file.  It will be created if it doesn't
     *  already exist.
     */
    sqlite3_enable_shared_cache (0);
    if ((status = sqlite3_open (dbfile, &db.handle))) {
	fprintf (stderr, "dts_dbOpen: Cannot connect to db '%s'\n", dbfile);
	return (ERR);
    } 


    /*  Execute the commands for creating the tables.
     */
    if (sqlite3_exec (db.handle, mk_entry_tbl, 0, 0, 0) ||
	sqlite3_exec (db.handle, mk_msg_tbl, 0, 0, 0)) {
	    fprintf (stderr, "dts_dbOpen: Cannot create db tables\n");
	    return (ERR);
    }

    /*  Execute the commands for creating the triggers, but only if we 
     *  are creating the table for the first time.
     */
    if (!exists && sqlite3_exec (db.handle, mk_entry_trigger, 0, 0, 0)) {
	fprintf (stderr, "dts_dbOpen: Cannot create db trigger\n");
	return (ERR);
    }

    /*  Create a dummy entry for index 0 that we use to mark the creation
     *  time of the log.  The 'size' will be kept as count of the number of
     *  entries.
     */
    dts_dbNewEntry ("DTS_Log", namebuf, namebuf, "", "", 0, "");

    db.dbfile = calloc (1, SZ_DBNAME);		/* save db file name	*/
    strcpy (db.dbfile, dbfile);

    return (OK);
}


/** 
 *  DTS_DBCREATE -- Create the database in the named file.  Existing table
 *  definition are dropped.
 *
 *  @brief	Create the database in the named file.
 *  @fn		int dts_dbCreate (char *dbfile)
 *
 *  @param	dbfile		database file name
 *  @return			status code (OK or ERR)
 */
int
dts_dbCreate (char *dbfile)
{
    char  namebuf[SZ_NAMEBUF];

    memset (namebuf, 0, SZ_NAMEBUF);
    gethostname (namebuf, SZ_NAMEBUF);

    if (dbfile) {
        if (access (dbfile, F_OK) < 0) {
	    fprintf (stderr, 
		"dts_dbCreate: DB file '%s' doesn't exist.\n", dbfile);
	    return (ERR);
        }

        sqlite3_enable_shared_cache (0);
        if ((status = sqlite3_open (dbfile, &db.handle))) {
	    /* Database connection failed.
	     */
	    fprintf (stderr, 
		"dts_dbCreate: Cannot connect to db '%s'\n", dbfile);
	    return (ERR);
        } 

    } else {
	/* Use the currently open DB handle. */
	if (!db.handle) {
	    fprintf (stderr, "dts_dbCreate: NULL db handle.\n");
	    return (ERR);
	}
    }


    /* Execute the queries to drop existing tables.
     */
    if (sqlite3_exec (db.handle, drop_entry_tbl, 0, 0, 0) ||
	sqlite3_exec (db.handle, drop_msg_tbl, 0, 0, 0)) {
	    fprintf (stderr, "dts_dbCreate: Cannot drop db tables\n");
	    return (ERR);
    }


    /* Execute the queries for creating the tables.
     */
    if (sqlite3_exec (db.handle, mk_entry_tbl, 0, 0, 0) ||
	sqlite3_exec (db.handle, mk_msg_tbl, 0, 0, 0)) {
	    fprintf (stderr, "dts_dbCreate: Cannot create db tables\n");
	    return (ERR);
    }

    /* Create a dummy entry for index 1 that we use to mark the creation
     * time of the log.  The 'size' will be kept as count of the number of
     * entries.
     */
    dts_dbNewEntry ("DTS_Log", namebuf, namebuf, "", "", 0, "");

    if (dbfile) {
    	memset (db.dbfile, 0, SZ_DBNAME);	/* save db file name	*/
    	strcpy (db.dbfile, dbfile);
    }

    return (OK);
}


/** 
 *  DTS_DBINIT -- Initialize the database, i.e. delete all existing entries.
 *
 *  @brief	Initialize the database, i.e. delete all existing entries.
 *  @fn		int dts_dbInit (void)
 *
 *  @return			status code (OK or ERR)
 */
int
dts_dbInit (void)
{
    int  status = -1;

/*    pthread_mutex_lock (&db_mut); */
    dts_dbCreate (NULL);
/*    pthread_mutex_unlock (&db_mut); */

    return ( status );
}


/** 
 *  DTS_DBCLOSE -- Close the connection to the currently open database.
 *
 *  @brief	Close the connection to the currently open database.
 *  @fn		int dts_dbClose (void)
 *
 *  @return			status code (OK or ERR)
 */
int
dts_dbClose (void)
{
    if (!db.handle)
	return (OK);

    if ((status = sqlite3_close (db.handle))) {
	fprintf (stderr, "dts_dbClose: %s\n", sqlite3_errmsg (db.handle));
	return (ERR);
    } 

    if (db.dbfile)
        free (db.dbfile);

    return (OK);
}


/*****************************************************************************/

/** 
 *  DTS_DBLOOKUP -- Lookup an entry by filename and/or queue name.  A list
 *  of keys is returned to allow callers to process multiple results.
 *
 *  @brief	Lookup an entry by filename and/or queue name.
 *  @fn		int *dts_dbLookup (char *fname, char *queue, int *nres)
 *
 *  @param	fname		requested file name
 *  @param	queue		requested queue name
 *  @param	nres		number of results
 *  @return			pointer to list of matched db keys
 */
Entry *
dts_dbLookup (char *fname, char *queue, int *nres)
{
    int    retval = 0, nrows = 0;
    char   query[SZ_SQL], *sql = NULL, *s;
    int    resSize = RES_INIT, res = 0;
    Entry *resList = calloc (RES_INIT, sizeof(Entry)), *e;


    if (!db.handle) {
	free ((void *) resList);
	return ((Entry *) NULL);
    }

    memset (query, 0, SZ_SQL);
    if (fname && queue) {
        sql = "SELECT * FROM entry WHERE "
	      "((hfname LIKE '%%%s%%') AND (queue LIKE '%%%s%%'));";
	sprintf (query, sql, fname, queue);

    } else if (fname) {
        sql = "SELECT * FROM entry WHERE (hfname LIKE '%%%s%%');";
	sprintf (query, sql, fname);

    } else if (queue) {
        sql = "SELECT * FROM entry WHERE (queue LIKE '%%%s%%');";
	sprintf (query, sql, queue);

    } else
        strcpy (query, "SELECT * FROM entry WHERE key > 0;");

    pthread_mutex_lock (&db_mut);

    /* Compile the query.
    **    ??? 
    **    CREATE TABLE new_table [AS] SELECT columns FROM table ...
    **    ??? 
    */
    if (sqlite3_prepare_v2 (db.handle, query, -1, &db.stmt, 0)) {
	fprintf (stderr, "dts_dbLookup: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	free ((void *) resList);
	return NULL;
    }


    /* Loop over result set.
     */
    *nres = 0;
    nrows = 0;
    while (1) {
	retval = sqlite3_step (db.stmt); 	/* fetch a row's status      */
	if (retval == SQLITE_ROW) { 		/* fetched a row 	     */

	    e = &resList[res++];
		
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_QUEUE) ))
		strncpy (e->queue, s, SZ_NAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_SRC) ))
		strncpy (e->src, s, SZ_NAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_DST) ))
		strncpy (e->dst, s, SZ_NAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_HFNAME) ))
		strncpy (e->hfname, s, SZ_FNAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_QPATH) ))
		strncpy (e->qpath, s, SZ_FNAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_MD5) ))
		strncpy (e->md5, s, SZ_MD5);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TIME_IN) ))
		strncpy (e->time_in, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TIME_OUT) ))
		strncpy (e->time_out, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TSTART) ))
		strncpy (e->tstart, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TEND) ))
		strncpy (e->tend, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TPURGED) ))
		strncpy (e->tpurged, s, SZ_DATE);

	    e->key	= (int)    sqlite3_column_int (db.stmt, E_KEY);
	    e->size	= (int)    sqlite3_column_int (db.stmt, E_SIZE);
	    e->nerrs	= (int)    sqlite3_column_int (db.stmt, E_NERRS);
	    e->status	= (int)    sqlite3_column_int (db.stmt, E_STATUS);
	    e->mbs	= (float)  sqlite3_column_double (db.stmt, E_MBS);

	    if (res > resSize) {
		Entry  *new;
		resSize += RES_INCR;
		new = calloc (resSize, sizeof (Entry));
		free (resList);
		resList = new;
	    }
	    *nres = ++nrows;

	} else if (retval == SQLITE_DONE) { 	/* all rows finished 	     */
	    break;

	} else { 				/* Some error encountered    */
	    fprintf (stderr, "dts_dbLookup: %s\n", sqlite3_errmsg (db.handle));
            pthread_mutex_unlock (&db_mut);
	    free ((void *) resList);
	    return (NULL);
	}
    }

    /* Finalize the query, destroy the prepared statement and return results.
     */
    sqlite3_finalize (db.stmt);
    pthread_mutex_unlock (&db_mut);

    return (resList);
}


/** 
 *  DTS_DBRAWQUERY -- Perform a raw query, returning pointer to entry records.
 *
 *  @brief	Perform a raw query, returning pointer to entry records.
 *  @fn		Entry *dts_dbRawQuery (char *where, int *nres)
 *
 *  @param	where		sql WHERE clause
 *  @param	nres		number of results found
 *  @return			handle to result set
 */
Entry *
dts_dbRawQuery (char *where, int *nres)
{
    int    retval = 0, nrows = 0;
    int    resSize = RES_INIT, res = 0;
    char   query[SZ_SQL], *s;
    Entry *resList = calloc (RES_INIT, sizeof(Entry)), *e;


    if (!db.handle) {
	free ((void *) resList);
	return ((Entry *) NULL);
    }

    memset (query, 0, SZ_SQL);
    sprintf (query, 
	"SELECT * FROM entry WHERE %s;", (where ? where : "key>0"));

    pthread_mutex_lock (&db_mut);

    /* Compile the query.
     */
    if (sqlite3_prepare_v2 (db.handle, query, -1, &db.stmt, 0)) {
	fprintf (stderr, "dts_dbRawQuery: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	free ((void *) resList);
	return NULL;
    }

    /* Loop over result set.
     */
    nrows = 0;
    while (1) {
	retval = sqlite3_step (db.stmt); 	/* fetch a row's status      */
	if (retval == SQLITE_ROW) { 		/* fetched a row 	     */
	    e = &resList[res++];
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_QUEUE) ))
		strncpy (e->queue, s, SZ_NAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_SRC) ))
		strncpy (e->src, s, SZ_NAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_DST) ))
		strncpy (e->dst, s, SZ_NAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_HFNAME) ))
		strncpy (e->hfname, s, SZ_FNAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_QPATH) ))
		strncpy (e->qpath, s, SZ_FNAME);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_MD5) ))
		strncpy (e->md5, s, SZ_MD5);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TIME_IN) ))
		strncpy (e->time_in, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TIME_OUT) ))
		strncpy (e->time_out, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TSTART) ))
		strncpy (e->tstart, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TEND) ))
		strncpy (e->tend, s, SZ_DATE);
	    if (( s = (char *) sqlite3_column_text (db.stmt, E_TPURGED) ))
		strncpy (e->tpurged, s, SZ_DATE);

	    e->key	= (int)    sqlite3_column_int (db.stmt, E_KEY);
	    e->size	= (int)    sqlite3_column_int (db.stmt, E_SIZE);
	    e->nerrs	= (int)    sqlite3_column_int (db.stmt, E_NERRS);
	    e->status	= (int)    sqlite3_column_int (db.stmt, E_STATUS);
	    e->mbs	= (float)  sqlite3_column_double (db.stmt, E_MBS);

	    if (res > resSize) {
		Entry  *new;
		resSize += RES_INCR;
		new = calloc (resSize, sizeof (Entry));
		free (resList);
		resList = new;
	    }
	    *nres = ++nrows;

	} else if (retval == SQLITE_DONE) { 	/* all rows finished 	     */
	    break;

	} else { 				/* Some error encountered    */
	    fprintf (stderr, "dts_dbRawQuery: %s\n", sqlite3_errmsg(db.handle));
            pthread_mutex_unlock (&db_mut);
	    return (NULL);
	}
    }

    /* Finalize the query, destroy the prepared statement and return results.
     */
    sqlite3_finalize (db.stmt);
    pthread_mutex_unlock (&db_mut);
    return (resList);
}


/** 
 *  DTS_DBRESFREE -- Perform a raw query, returning pointer to entry records.
 *
 *  @brief	Perform a raw query, returning pointer to entry records.
 *  @fn		int dts_dbResFree (int res)
 *
 *  @param	res		result set handle
 *  @return			status flag
 */
int
dts_dbResFree (int res)
{
    return (OK);
}



/*****************************************************************************/

/** 
 *  DTS_DBNEWENTRY -- Create a new file entry in the DB.
 *
 *  @brief	Create a new file entry in the DB.
 *  @fn		int dts_dbNewEntry (char *queue, char *src, char *dst, 
 *		    char *hfname, char *qpath, long size, char *md5)
 *
 *  @param	queue		queue name
 *  @param	src		source host name
 *  @param	dst		destination host name
 *  @param	hfname		original node!/path/file.fits name
 *  @param	qpath		path to local queue directory
 *  @param	size		file size (bytes)
 *  @param	md5		MD5 checksum
 *  @return			ID key of new entry or ERR
 */
int
dts_dbNewEntry (char *queue, char *src, char *dst, char *hfname, 
	char *qpath, long size, char *md5)
{
    Entry *e;
    int   nres;
    char  sql[SZ_SQL];
    char *sql1 = "INSERT INTO entry "
	"(queue,src,dst,hfname,qpath,size,md5,nerrs,time_in,status)";


    if (!db.handle)
	return (OK);

    /* If we're creating the DB entry and there already is one, return.
     */
    if (strcmp (queue, "DTS_Log") == 0) {
        e = dts_dbLookup (NULL, "DTS_Log", &nres);
	if (e) free (e);
        if (nres > 0)
	    return (OK);
    }

    pthread_mutex_lock (&db_mut);

    memset (sql, 0, SZ_SQL);
    sprintf (sql, 
	"%s VALUES ('%s','%s','%s','%s','%s',%ld,'%s',0,DATETIME('NOW'),0);",
	sql1, queue, src, dst, hfname, qpath, size, md5);

    /* Execute the queries for creating the tables.
     */
    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbNewEntry: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    /* Set the no. of records         		-- FIXME -- 
    if ((id = sqlite3_last_insert_rowid (db.handle)) > 0) {
        pthread_mutex_unlock (&db_mut);
        dts_dbSetInt (1, E_SIZE, id-1);
	return ((int) id);
    }
     */

    pthread_mutex_unlock (&db_mut);
    return (ERR);
}


/** 
 *  DTS_DBDELENTRY -- Delete the named entry from the database.
 *
 *  @brief	Delete the named entry from the database.
 *  @fn		int dts_dbDelEntry (int key)
 *
 *  @param	key		db entry key
 *  @return			status code (OK or ERR)
 */
int
dts_dbDelEntry (int key)
{
    char  sql[SZ_SQL];

    if (!db.handle)
	return (OK);

    pthread_mutex_lock (&db_mut);

    /* Execute the command to delete the entry.
     */
    memset (sql, 0, SZ_SQL);
    sprintf (sql, "DELETE FROM entry WHERE key=%d;", key);
    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbDelEntry: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    /* Execute the command to delete the messages associated with the key.
     */
    memset (sql, 0, SZ_SQL);
    sprintf (sql, "DELETE FROM messages WHERE ekey=%d;", key);
    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbDelEntry: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/** 
 *  DTS_DBPURGE -- Purge entries where the ingest time is older than the
 *  specified date.  The reserved string 'now' may be used to represent
 *  the current time.  Note the DB stores all times in UTC.
 *
 *  @brief	Purge entries older than specified date
 *  @fn		int dts_dbPurge (long epoch)
 *
 *  @param	epoch		unix epoch (seconds since 1/1/70)
 *  @return			status code (OK or ERR)
 */
int
dts_dbPurge (long epoch)
{
    char  sql[SZ_SQL];


    if (!db.handle)
	return (OK);

    pthread_mutex_lock (&db_mut);

    /* Execute the command to delete the entry.
     */
    memset (sql, 0, SZ_SQL);
    sprintf (sql, "DELETE FROM entry WHERE DATETIME(time_in,'unixepoch')<=%d;",
	(int) epoch);
    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbDelEntry: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    /* Execute the command to delete the messages associated with the key.
     */
    memset (sql, 0, SZ_SQL);
    sprintf (sql, "DELETE FROM messages WHERE DATETIME(mtime,'unixepoch')<=%d;",
	(int) epoch);
    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbDelEntry: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/*****************************************************************************/

/** 
 *  DTS_DBSETINT -- Set an integer value for the specified entry.
 *
 *  @brief	Set an integer value for the specified entry.
 *  @fn		int dts_dbSetInt (int key, int field, int ival)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @param	ival		value
 *  @return			status code (OK or ERR)
 */
int
dts_dbSetInt  (int key, int field, int ival)
{
    char sql[SZ_SQL];

    if (!db.handle)
	return (OK);

    if (field != E_SIZE && field != E_NERRS && field != E_STATUS) {
	if (db.debug)
	    fprintf (stderr, "dts_dbSetInt: invalid field %d\n", field);
	return (ERR);
    }

    pthread_mutex_lock (&db_mut);

    memset (sql, 0, SZ_SQL);
    sprintf (sql, "UPDATE entry SET %s=%d WHERE key=%d;", 
	ke_fields[field], ival, key);

    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbSetInt: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/** 
 *  DTS_DBSETFLOAT -- Set a real value for the specified entry.
 *
 *  @brief	Set a real value for the specified entry.
 *  @fn		int dts_dbSetFloat (int key, int field, float rval)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @param	rval		value
 *  @return			status code (OK or ERR)
 */
int
dts_dbSetFloat  (int key, int field, float rval)
{
    char sql[SZ_SQL];

    if (!db.handle)
	return (OK);

    if (field != E_MBS) {
	if (db.debug)
	    fprintf (stderr, "dts_dbSetFloat: invalid field %d\n", field);
	return (ERR);
    }

    pthread_mutex_lock (&db_mut);

    memset (sql, 0, SZ_SQL);
    sprintf (sql, "UPDATE entry SET %s=%g WHERE key=%d;", 
	ke_fields[field], rval, key);

    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbSetFloat: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/** 
 *  DTS_DBSETSTR -- Set a string value for the specified entry.
 *
 *  @brief	Set a string value for the specified entry.
 *  @fn		int dts_dbSetStr (int key, int field char *str)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @param	str		value
 *  @return			status code (OK or ERR)
 */
int
dts_dbSetStr  (int key, int field, char *str)
{
    char sql[SZ_SQL];

    if (!db.handle)
	return (OK);

    if (field==E_SIZE || field==E_NERRS || field==E_STATUS || field==E_MBS) {
	if (db.debug)
	    fprintf (stderr, "dts_dbSetStr: invalid field %d\n", field);
	return (ERR);
    }

    pthread_mutex_lock (&db_mut);

    memset (sql, 0, SZ_SQL);
    sprintf (sql, "UPDATE entry SET %s='%s' WHERE key=%d;", 
	ke_fields[field], str, key);

    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
        fprintf (stderr, "dts_dbSetStr: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/** 
 *  DTS_DBGETINT -- Get an integer field from an entry.
 *
 *  @brief	Get an integer field from an entry.
 *  @fn		int dts_dbGetInt (int key, int field)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @return			int value
 */
int
dts_dbGetInt  (int key, int field)
{
    char   query[SZ_SQL];
    float  val=0.0;


    if (!db.handle)
	return (OK);

    if (field != E_SIZE && field != E_NERRS && field != E_STATUS) {
	if (db.debug)
	    fprintf (stderr, "dts_dbGetInt: invalid field %d\n", field);
	return (ERR);
    }

    memset (query, 0, SZ_SQL);
    sprintf (query, "SELECT %s FROM entry where key=%d;", 
	ke_fields[field], key);

    pthread_mutex_lock (&db_mut);

    /* Compile the query.
     */
    if (sqlite3_prepare_v2 (db.handle, query, -1, &db.stmt, 0)) {
        fprintf (stderr, "dts_dbGetInt: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
        return (ERR);
    }

    if (sqlite3_step (db.stmt) == SQLITE_ROW)
	val = (int) sqlite3_column_int (db.stmt,0);

    /* Finalize the query and destroy the prepared statement.
     */
    sqlite3_finalize (db.stmt);

    pthread_mutex_unlock (&db_mut);
    return (val);
}


/** 
 *  DTS_DBGETFLOAT -- Get a real-valued field from an entry.
 *
 *  @brief	Get a real-valued field from an entry.
 *  @fn		int dts_dbGetFloat (int key, int field)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @return			int value
 */
float
dts_dbGetFloat  (int key, int field)
{
    char   query[SZ_SQL];
    float  val=0.0;

    if (!db.handle)
	return (0.0);

    if (field != E_MBS) {
	if (db.debug)
	    fprintf (stderr, "dts_dbGetFloat: invalid field %d\n", field);
	return (0.0);
    }

    memset (query, 0, SZ_SQL);
    sprintf (query, "SELECT %s FROM entry where key=%d;", 
	ke_fields[field], key);

    pthread_mutex_lock (&db_mut);

    /* Compile the query.
     */
    if (sqlite3_prepare_v2 (db.handle, query, -1, &db.stmt, 0)) {
        fprintf (stderr, "dts_dbGetFloat: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
        return ((float) ERR);
    }

    if (sqlite3_step (db.stmt) == SQLITE_ROW)
	val = (float) sqlite3_column_double (db.stmt,0);

    /* Finalize the query and destroy the prepared statement.
     */
    sqlite3_finalize (db.stmt);

    pthread_mutex_unlock (&db_mut);
    return (val);
}


/** 
 *  DTS_DBGETSTR -- Get a string valued field from an entry.
 *
 *  @brief	Get a string valued field from an entry.
 *  @fn		char *dts_dbGetStr (int key, int field)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @return			string value
 */
char *
dts_dbGetStr  (int key, int field)
{
    char   query[SZ_SQL];
    static char str[SZ_SQL];

    if (!db.handle)
	return (NULL);

    if (field==E_SIZE || field==E_NERRS || field==E_STATUS || field==E_MBS) {
	if (db.debug)
	    fprintf (stderr, "dts_dbGetStr: invalid field %d\n", field);
	return (NULL);
    }

    memset (query, 0, SZ_SQL);
    sprintf (query, "SELECT %s FROM entry where key=%d;", 
	ke_fields[field], key);

    pthread_mutex_lock (&db_mut);

    /* Compile the query.
     */
    if (sqlite3_prepare_v2 (db.handle, query, -1, &db.stmt, 0)) {
        fprintf (stderr, "dts_dbGetInt: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
        return NULL;
    }

    memset (str, 0, SZ_SQL);
    if (sqlite3_step (db.stmt) == SQLITE_ROW)
	strcpy (str, (char *) sqlite3_column_text (db.stmt, 0));

    /* Finalize the query and destroy the prepared statement.
     */
    sqlite3_finalize (db.stmt);

    pthread_mutex_unlock (&db_mut);
    return (str);
}



/*****************************************************************************/

/** 
 *  DTS_DBSETTIME -- Set a time value for the specified entry.
 *
 *  @brief	Set a time value for the specified entry.
 *  @fn		int dts_dbSetTime (int key, int field)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @return			status code (OK or ERR)
 */
int
dts_dbSetTime  (int key, int field)
{
    char sql[SZ_SQL];


    if (!db.handle)
	return (OK);
    if (field < 0 || field > DTS_TPURGE) {
	if (db.debug)
	    fprintf (stderr, "dts_dbSetTime: invalid field %d\n", field);
	return (ERR);
    }

    pthread_mutex_lock (&db_mut);

    memset (sql, 0, SZ_SQL);
    sprintf (sql, 
	"UPDATE entry SET %s=strftime('%%Y-%%m-%%d %%H:%%M:%%f','now') "
	"WHERE key=%d;", tkeys[field], key);

    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
	if (db.debug)
            fprintf (stderr, "dts_dbSetTime: Cannot update '%s' timestamp\n",
	        tkeys[field]);
        pthread_mutex_unlock (&db_mut);
        return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/** 
 *  DTS_DBGETTIME -- Get a time value for the specified entry.
 *
 *  @brief	Get a time value for the specified entry.
 *  @fn		tstr = dts_dbGetTime (int key, int field, int local)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @param	local		get local time instead of UT?
 *  @return			status code (OK or ERR)
 */
char *
dts_dbGetTime  (int key, int field, int local)
{
    char  sql[SZ_SQL];
    static char tstr[SZ_SQL];


    if (!db.handle)
	return (NULL);

    /*  Execute and compile the query to get the time.
     */
    memset (sql, 0, SZ_SQL);
    if (!local)
        sprintf (sql, "SELECT %s FROM entry WHERE key=%d;", 
 	    tkeys[field], key);
    else 
        sprintf (sql, 
	    "SELECT datetime(%s,'localtime') FROM entry WHERE key=%d;", 
 	    tkeys[field], key);

    pthread_mutex_lock (&db_mut);

    if (sqlite3_prepare_v2 (db.handle, sql, -1, &db.stmt, 0)) {
        fprintf (stderr, "dts_dbGetTime: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
        return (NULL);
    }

    memset (tstr, 0, SZ_SQL);
    if (sqlite3_step (db.stmt) == SQLITE_ROW)
	strcpy (tstr, (char *) sqlite3_column_text (db.stmt, 0));

    /* Finalize the query and destroy the prepared statement.
     */
    sqlite3_finalize (db.stmt);

    /*  Note that the value we return may have side effects when multiple
     *,  calls to this procedure are made.
     */
    pthread_mutex_unlock (&db_mut);
    return (tstr);
}


/** 
 *  DTS_DBGEEPOCH -- Get a time value (as an epoch) for the specified entry.
 *
 *  @brief	Get a time value (as an epoch) for the specified entry.
 *  @fn		int dts_dbGetEpoch (int key, int field, int local)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @param	local		get local time instead of UT?
 *  @return			status code (OK or ERR)
 */
int
dts_dbGetEpoch  (int key, int field, int local)
{
    char sql[SZ_SQL];
    int  epoch = -1;


    if (!db.handle)
	return (OK);

    /*  Execute and compile the query to get the time.
     */
    memset (sql, 0, SZ_SQL);
    if (!local)
        sprintf (sql, "SELECT strftime('%%s',%s) FROM entry WHERE key=%d\n", 
 	    tkeys[field], key);
    else
        sprintf (sql, 
	    "SELECT strftime('%%s',%s,'localtime') FROM entry WHERE key=%d\n", 
 	    tkeys[field], key);

    pthread_mutex_lock (&db_mut);

    if (sqlite3_prepare_v2 (db.handle, sql, -1, &db.stmt, 0)) {
        fprintf (stderr, "dts_dbGetTime: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
        return (ERR);
    }

    if (sqlite3_step (db.stmt) == SQLITE_ROW) {
	epoch = sqlite3_column_int (db.stmt, 0);
    } else {
        fprintf (stderr, "dts_dbGetTime: %s\n", sqlite3_errmsg (db.handle));
        pthread_mutex_unlock (&db_mut);
        return (ERR);
    }

    /* Finalize the query and destroy the prepared statement.
     */
    sqlite3_finalize (db.stmt);

    pthread_mutex_unlock (&db_mut);
    return (epoch);
}



/*****************************************************************************/

/** 
 *  DTS_DBADDMSG -- Add a message for the name entry key.
 *
 *  @brief	Add a message for the name entry key.
 *  @fn		int dts_dbAddMsg (int key, char *msg)
 *
 *  @param	key		db entry key
 *  @param	field		field type code
 *  @return			status code (OK or ERR)
 */
int
dts_dbAddMsg  (int key, char *msg)
{
    char  sql[SZ_SQL], *ip;
    char *sql1 = "INSERT INTO messages (ekey,mtime,mstr) ";
    int    res;


    if (! db.handle || key == 1)
	return (OK);

    pthread_mutex_lock (&db_mut);

    memset (sql, 0, SZ_SQL);
    sprintf (sql, "%s VALUES (%d,DATETIME('NOW'),\"%s\");", sql1, key, msg);

    for (ip=sql; *ip; ip++)		/* strip newlines	*/
	if (*ip == '\n')
	    *ip = ' ';

    /* Execute the queries for creating the tables.
     */
    if ((res = sqlite3_exec (db.handle, sql, 0, 0, 0))) {
        fprintf (stderr, "dts_dbAddMsg: (%d) %s, res=%d\n", 
	    key, sqlite3_errmsg (db.handle), res);
        pthread_mutex_unlock (&db_mut);
	return (ERR);
    }

    pthread_mutex_unlock (&db_mut);
    return (OK);
}


/** 
 *  DTS_DBGETMSGS -- Get all messages associated with the entry.
 *
 *  @brief	Get all messages associated with the entry.
 *  @fn		int dts_dbGetMsgs (int key)
 *
 *  @param	key		db entry key
 *  @return			pointer to message strings
 */
Msg *
dts_dbGetMsgs (int key)
{
    char  sql[SZ_SQL], *restbl = NULL;
    int   nrows, retval;
    Msg  *msgArray = calloc (MAX_MSGS, sizeof (Msg)), *m;


    if (!db.handle) {
        if (db.debug) fprintf (stderr, "dts_dbGetMsgs: invalid db handle\n");
        return (NULL);
    }

    memset (sql,    0, SZ_SQL);		/* initialize		*/

    /*  Execute and compile the query.
     */
    restbl = dts_dbInitResult ();

    sprintf (sql, "SELECT * FROM messages WHERE ekey=%d;", key);

    pthread_mutex_lock (&db_mut);

    if (sqlite3_prepare_v2 (db.handle, sql, -1, &db.stmt, 0)) {
        fprintf (stderr, "dts_dbGetMsgs: %s\n", sqlite3_errmsg (db.handle));
        return (NULL);
    }


    /* Clear result array.
     */
    memset (msgArray, 0, (MAX_MSGS * sizeof(Msg)));

    /* Loop over result set.
     */
    nrows = 0;
    while (1) {
        retval = sqlite3_step (db.stmt);        /* fetch a row's status */
        if (retval == SQLITE_ROW) {             /* fetched a row        */
	    m = &msgArray[nrows++];
	    m->key   = (int)    sqlite3_column_int  (db.stmt, M_KEY);
	    m->ekey  = (int)    sqlite3_column_int  (db.stmt, M_EKEY);
	    strncpy (m->mtime, 
		(char *) sqlite3_column_text (db.stmt, M_MTIME), SZ_DATE);
	    strncpy (m->mstr,  
		(char *) sqlite3_column_text (db.stmt, M_MSTR), SZ_MSG);

        } else if (retval == SQLITE_DONE) {     /* all rows finished */
            break;
        } else {
            fprintf (stderr, 
		"dts_dbGetMsgs Error: %s\n", sqlite3_errmsg (db.handle));
    	    pthread_mutex_unlock (&db_mut);
            return (NULL);
        }
    }

    /* Finalize the query and destroy the prepared statement.
     */
    sqlite3_finalize (db.stmt);

    pthread_mutex_unlock (&db_mut);
    return (msgArray);
}



/*****************************************************************************
**  Local or debug procedures.
******************************************************************************/

static char * 
dts_dbInitResult (void)
{
    char sql[SZ_SQL];
    static char resTable[SZ_FNAME];

    memset (resTable, 0, SZ_FNAME);
    sprintf (resTable, "res%d", ++reskey);

    /*  'reskey' is now the key for the active result set.
    **  If the table already exists, drop it from the database.
    */
    
    memset (sql, 0, SZ_SQL);
    sprintf (sql, "DROP TABLE IF EXISTS %s;", resTable);

    pthread_mutex_lock (&db_mut);

    if (sqlite3_exec (db.handle, sql, 0, 0, 0)) {
	if (db.debug)
            fprintf (stderr, "dts_dbInitResult: Can't drop result table '%s'\n",
	        resTable);
    }

    pthread_mutex_unlock (&db_mut);
    return (resTable);
}

/** 
 *  DTS_DBSETDEBUG -- Set the inteface debug flag.
 *
 *  @brief	Set the inteface debug flag.
 *  @fn		int dts_dbSetDebug (int val)
 *
 *  @param	val		debug value
 *  @return			pointer to message strings
 */
int
dts_dbSetDebug (int val)
{
    db.debug = val;
    return (OK);
}
