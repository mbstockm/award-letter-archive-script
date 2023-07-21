import groovy.sql.Sql
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.Blob
import java.sql.ResultSet

@Grab(group = 'com.oracle',module = 'ojdbc8', version = 'current')

def aidy = '1920'
def list = []
def completedAlready = []
/**
 * ArchiveAwardLetters.groovy
 * Michael Stockman
 * Script copies award letter PDF blobs from Oracle to windows file share to free up Oracle tablespace
 * Please check aidy code being run before executing
 * Script does not do the final delete at this time. Verify completion and then perform delete on RYRAWRF for the aid year run
 * Designed to be run by user on their workstation with access to fileshare
 * If an error occurs where user gets disconnected from the database or the fileshare the script may need to be run again to pick up all PDFs
 * A file is created storing name of already downloaded PDF, these results are stored in an Oracle private temporary table so the main query doesn't process them again.
 */
Sql.withInstance(dbProps()) { Sql sql ->
    sql.connection.autoCommit = false

    createTempTab(sql)
    completedAlready = completedAlreadyFromFile(aidy)
    insertAlreadyCompleted(sql,completedAlready)

    list = findAwardLetterRecords(sql,aidy)

    Paths.get('U:\\SFS - Award Letters',aidy,aidy + '.txt').withWriterAppend { BufferedWriter bw ->
        list.each {
            Path outputPath = Paths.get('U:\\SFS - Award Letters',it.aidy,it.ltrc,it.id + '.pdf')
            outputPath.createParentDirectories()

            println it.aidy + it.ltrc + it.id + '.pdf'

            Blob blob = getAwardLetterBlob(sql,it.pidm,it.aidy,it.ltrc)
            if (blob != null) {
                Files.copy(
                        blob.getBinaryStream(),
                        outputPath,
                        StandardCopyOption.REPLACE_EXISTING
                )
            }

            bw.append(it.aidy + it.ltrc + it.id + '.pdf').append(System.properties."line.separator")
        }
    }
    sql.commit();
}

/**
 * Queries award letters table for all letters in the input aid year as long as they do not exist in the already moved temp table
 * @param sql
 * @param aidy
 * @return
 */
def findAwardLetterRecords(Sql sql, def aidy) {
    return sql.rows("""
              select f.ryrawrf_pidm pidm
                    ,i.spriden_id id
                    ,f.ryrawrf_aidy_code aidy
                    ,f.ryrawrf_ltrc ltrc
                from ryrawrf f,spriden i
               where i.spriden_change_ind is null
                 and i.spriden_pidm = f.ryrawrf_pidm
                 and f.ryrawrf_aidy_code = ${aidy}
                  and not exists (select 1 from ora\$ptt_ryrawrf_mv where banner_id = i.spriden_id and aidy = f.ryrawrf_aidy_code and ltrc = f.ryrawrf_ltrc)
              order by f.ryrawrf_aidy_code, i.spriden_id, f.ryrawrf_ltrc
            """)
}

/**
 * Pulls the binary object for the award letter PDF based on the primary key from the award letter table
 * @param sql
 * @param pidm
 * @param aidy
 * @param ltrc
 * @return
 */
Blob getAwardLetterBlob(Sql sql, def pidm, def aidy, def ltrc) {
    Blob blob
    sql.query("""select ryrawrf_file blob from ryrawrf where ryrawrf_pidm = ? and ryrawrf_aidy_code = ? and ryrawrf_ltrc = ?""",
            [pidm,aidy,ltrc],
            { ResultSet rs ->
        if (rs.next()) {
            blob = rs.getBlob("blob")
        }
    })
    return blob
}

/**
 * Executes DDL to create Oracle private temporary table that drops on commit
 * @param sql
 * @return
 */
def createTempTab(Sql sql) {
    sql.execute("""
        create private temporary table ora\$ptt_ryrawrf_mv (
            aidy varchar2(4)
           ,ltrc varchar2(4)
           ,banner_id varchar2(9)
        ) on commit drop definition
       """)
}

/**
 * Insert results of already completed collection into Oracle private temporary table
 * @param sql
 * @param completedAlready
 * @return
 */
def insertAlreadyCompleted(Sql sql, def completedAlready) {
    def aidy
    def ltrc
    def bannerId

    if (completedAlready.size() == 0) {
        return false
    }

    sql.withBatch(25,"""insert into ora\$ptt_ryrawrf_mv(aidy,ltrc,banner_id) values (?,?,?)""") { bs ->
        completedAlready.each { String l ->
            aidy = l.substring(0,4)
            ltrc = l.substring(4,8)
            bannerId = l.substring(8,17)
            bs.addBatch([aidy,ltrc,bannerId])
        }
    }
}

/**
 * Looks up existing aid year already completed text file and parses results and creates collection
 * @param aidy
 * @return
 */
def completedAlreadyFromFile(aidy) {
    def completedAlready = []
    Path completedAlreadyFile = Paths.get('U:\\SFS - Award Letters',aidy,aidy + '.txt')
    if (!Files.exists(completedAlreadyFile)) {
        completedAlreadyFile.createParentDirectories()
        Files.createFile(completedAlreadyFile)
    }

    Paths.get('U:\\SFS - Award Letters',aidy,aidy + '.txt').withReader { BufferedReader br ->
        String line
        while ((line = br.readLine()) != null) {
            completedAlready << line.take(line.indexOf('.'))
        }
    }
    return completedAlready
}

/**
 * Database props object
 * @return
 */
def dbProps() {
    def properties = new Properties()
    Paths.get(System.properties.'user.home','.credentials','bannerProduction.properties').withInputStream {
        properties.load(it)
    }
    return properties
}
