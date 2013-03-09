
module.exports = {
	db: {
	  database: "plm-media-manager-dev0"
		,local: {
			 host: "localhost"
			,port: "5984"
			,type: "couchdb"
		}
		,remote: {
			host: "72.52.106.218"
		  ,port: undefined
		}
	},
  loadTest: {
    importPath: './test/resources/images'
    ,genCheckSums: false
    ,numJobs: 1
  }

}
