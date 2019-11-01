/**
 *  DTSDB.H -- Header file for the DTS database interface.
 *
 *  @file       dtsdb.h
 *  @author     Mike Fitzpatrick
 *  @date       1/10/11
 *
 *  @brief  Header file for the DTS database interface.
 */

#ifndef SZ_DBNAME
#define SZ_DBNAME               512
#endif
#ifndef SZ_SQL
#define SZ_SQL                	1024
#endif
#ifndef SZ_FNAME
#define SZ_FNAME                128
#endif
#ifndef OK
#define OK                      0
#endif
#ifndef ERR
#define ERR                     -1
#endif


#define	SZ_DATE			18
#define	SZ_MSG			160
#define	SZ_NAME			32
#define	SZ_MD5			64

#define MAX_MSGS                64              /* max no. returned msgs    */
#define RES_INIT                1024            /* initial size of res list */
#define RES_INCR                12              /* result list increment    */


#define DTS_TIME_IN             0               /* time entry was created   */
#define DTS_TIME_OUT            1               /* time entry was completed */
#define DTS_TSTART              2               /* transfer start time      */
#define DTS_TEND                3               /* transfer end time        */
#define DTS_TPURGE              4               /* time file was deleted    */

#define FMT_EPOCH               0               /* time in seconds (UT)     */
#define FMT_CTIME               1               /* calendar time (UT)       */
#define FMT_LEPOCH              2               /* local time in seconds    */
#define FMT_LTIME               3               /* local calendar time      */

#define E_KEY                   0		/* entry table 		    */
#define E_QUEUE                 1
#define E_SRC                   2
#define E_DST                   3
#define E_HFNAME                4
#define E_QPATH                 5
#define E_SIZE                  6
#define E_MD5                   7
#define E_NERRS                 8
#define E_TIME_IN               9
#define E_TIME_OUT              10
#define E_TSTART                11
#define E_TEND                  12
#define E_TPURGED               13
#define E_TTIME                 14
#define E_MBS                   15
#define E_STATUS                16

#define M_KEY                   0		/* message table 	    */
#define M_EKEY                  1
#define M_MTIME                 2
#define M_MSTR                  3


typedef struct {
  int	 key;					/* primary index key	    */
  int	 ekey;					/* entry index key	    */
  char	 mtime[SZ_DATE];			/* message timestamp	    */
  char	 mstr[SZ_MSG];				/* message string	    */
} Msg, *MsgP;

typedef struct {
  int	 key;					/* primary index key  	    */
  char	 queue[SZ_NAME];			/* queue name 		    */
  char	 src[SZ_NAME];				/* source host name 	    */
  char	 dst[SZ_NAME];				/* destination host name    */
  char	 hfname[SZ_FNAME];			/* original host /path/name */
  char	 qpath[SZ_FNAME];			/* local queue path 	    */
  int    size;					/* size of file (bytes)     */
  char	 md5[SZ_MD5];				/* MD5 checksum 	    */
  int    nerrs;					/* number of errors found   */
  char	 time_in[SZ_DATE];			/* time entered the queue   */
  char	 time_out[SZ_DATE];			/* time queue completed     */
  char	 tstart[SZ_DATE];			/* transfer start time      */
  char	 tend[SZ_DATE];				/* transfer end time 	    */
  char	 tpurged[SZ_DATE];			/* time deleted from queue  */
  float  mbs;					/* transfer mbs 	    */
  int    status;				/* status flag 	    	    */

  int    nmsgs;					/* number of messages  	    */
  Msg   *messages;				/* message array  	    */
} Entry, *EntryP;



/*******************************
**    Interface prototypes    **
*******************************/

int 	dts_dbOpen (char *dbfile);
int 	dts_dbClose (void);
int 	dts_dbCreate (char *dbfile);
int 	dts_dbInit (void);
int 	dts_dbPurge (long epoch);

Entry  *dts_dbLookup (char *fname, char *queue, int *nres);
Entry  *dts_dbRawQuery (char *where, int *nres);
int 	dts_dbNewEntry (char *queue, char *src, char *dst, char *hfname, 
			    char *qpath, long size, char *md5);
int 	dts_dbDelEntry  (int key);

int 	dts_dbSetInt (int key, int field, int ival);
int 	dts_dbGetInt (int key, int field);
int 	dts_dbSetFloat (int key, int field, float rval);
float 	dts_dbGetFloat (int key, int field);
int 	dts_dbSetStr (int key, int field, char *str);
char   *dts_dbGetStr (int key, int field);

int 	dts_dbSetTime (int key, int field);
char   *dts_dbGetTime (int key, int field, int local);
int 	dts_dbGetEpoch (int key, int field, int local);

int 	dts_dbAddMsg (int key, char *msg);
Msg    *dts_dbGetMsgs (int key);

