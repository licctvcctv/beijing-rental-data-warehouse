param(
    [string]$RawDir = "../crawler/data/export",
    [string]$OutputDir = "target/local-offline-output"
)

$BaseDir = Split-Path -Parent $PSScriptRoot
Push-Location $BaseDir
try {
    mvn -q -DskipTests -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 package
    java -cp "target/wenyu-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar" `
        com.beijing.wenyu.runner.LocalOfflineWarehouseRunner `
        $RawDir `
        $OutputDir
}
finally {
    Pop-Location
}
