<html>
    <head>
        <meta content="text/html; charset=UTF-8" http-equiv="content-type" />
    </head>
    <body style="width: 100%;" class="c2 doc-content">
        <p class="c6 c32 title" id="h.j5qaxkjyceud"><span class="c25"><h1>Infoworks Custom Target for MongoDB</h1></span></p>
        <p class="c0 c4"><span class="c1"></span></p>
        <p class="c0 c6"><span class="c11">Infoworks custom target solution to write the pipeline data to MongoDB in OVERWRITE/APPEND/MERGE mode.</span></p>
        <h1 class="c24 c6" id="h.pjv8tujelwqn"><span class="c15 c17">Step 1: Create Pipeline Extension</span></h1>
        <p class="c0 c6"><span class="c11">Download the jars from the below link and upload them as shown below</span></p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span class="c26">Admin &gt; Extensions &gt; Pipeline Extension &gt; Create pipeline Extension</span>
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 82.67px;">
                <img alt="" src="images/image3.png" style="width: 100%; height: 82.67px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 376px;">
                <img alt="" src="images/image6.png" style="width: 624px; height: 376px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <p class="c0 c4"><span class="c1"></span></p>
        <p class="c0 c4"><span class="c1"></span></p>
        <p class="c0 c4"><span class="c1"></span></p>
        <h1 class="c24 c6" id="h.wyfbk81uk61h"><span class="c15 c17">Step 2: Add Pipeline Extension to Domain</span></h1>
        <p class="c0 c4"><span class="c1"></span></p>
        <p class="c0 c6">
            <span class="c18">Go to</span><span class="c13">&nbsp;</span><span class="c26">Manage Domains</span><span class="c13">&nbsp;&gt; </span><span class="c26">Edit the domain with the pipeline</span>
            <span class="c13">&nbsp;&gt; </span><span class="c26">click on manage artifacts</span><span class="c13">&nbsp;&gt; </span><span class="c15 c26 c31">Add the pipeline extension to the domain</span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 106.67px;">
                <img alt="" src="images/image5.png" style="width: 100%; height: 106.67px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <h1 class="c6 c24" id="h.lzvb045f7fxg"><span class="c15 c17">Step 3: Configure the Custom Target using the Pipeline Extension</span></h1>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 308px;">
                <img alt="" src="images/image1.png" style="width: 100%; height: 308px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6"><span class="c31 c35">Available keys to configure</span></p>
        <p class="c0 c4"><span class="c8"></span></p>
        <a id="t.49fb44be778d2ddaed26b13cd2e96313fdd01ebd"></a><a id="t.0"></a>
        <table class="c47">
            <tr class="c50">
                <td class="c39" colspan="1" rowspan="1">
                    <p class="c23"><span class="c42">KEY</span></p>
                </td>
                <td class="c37" colspan="1" rowspan="1">
                    <p class="c23"><span class="c42">DESCRIPTION</span></p>
                </td>
            </tr>
            <tr class="c5">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">uri</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">Pass the mongodb uri in the format: (Note: password field will always remain </span><span class="c15 c20">***)</span></p>
                    <p class="c0"><span class="c7">mongodb://{username}:</span><span class="c20">***</span><span class="c7">@{hostname}:{port}/?authSource={auth_db}</span></p>
                </td>
            </tr>
            <tr class="c22">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">encryptedPassword</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">Pass the encrypted password by running the command in edge node (IN11**rk is the actual password)</span></p>
                    <p class="c0"><span class="c15 c7">bash /opt/infoworks/apricot-meteor/infoworks_python/infoworks/bin/infoworks_security.sh -encrypt -p IN11**rk</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">database</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">Database to which we need to connect</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">collection</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">Collection to which the data has to be written</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">mode</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">overwrite/append/merge</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">merge_key</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">Comma separated column names on which merge has to be performed</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">index_key</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">Column name on which index has to be created</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c16" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">table_exists</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">true/false. If True the collection and index won&#39;t be dropped from MongoDB</span></p>
                </td>
            </tr>
        </table>
        <p class="c0 c4"><span class="c1"></span></p>
        <p class="c0 c4"><span class="c1"></span></p>
        <h1 class="c24 c6" id="h.5008bpier8dl"><span>Notes:</span></h1>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c6"><span class="c36">Jars to upload can be found here: </span></p>
        <p class="c0 c4"><span class="c36"></span></p>
        <p class="c0 c6">
            <span class="c13 c27">
                <a class="c34" href="https://www.google.com/url?q=https://infoworks-releases.s3.amazonaws.com/MongoTarget.zip&amp;sa=D&amp;source=editors&amp;ust=1713324872103254&amp;usg=AOvVaw27RQY0WuUHvpAjZ8nESFqv">
                    https://infoworks-releases.s3.amazonaws.com/MongoTarget.zip
                </a>
            </span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c6"><span class="c30">References:</span></p>
        <p class="c0 c6">
            <span class="c27 c13">
                <a
                    class="c34"
                    href="https://www.google.com/url?q=https://www.mongodb.com/docs/spark-connector/current/configuration/%23std-label-spark-output-conf&amp;sa=D&amp;source=editors&amp;ust=1713324872103838&amp;usg=AOvVaw2W-ztnwTSpN-wP9HcSE5ll"
                >
                    https://www.mongodb.com/docs/spark-connector/current/configuration/#std-label-spark-output-conf
                </a>
            </span>
        </p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c13 c14"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c32 c6 title" id="h.eb9by9xp9zr0"><span class="c25"><h1>Infoworks Custom Target for DynamoDB</h1></span></p>
        <p class="c0 c4"><span class="c11"></span></p>
        <p class="c0 c6"><span class="c11">Infoworks custom target solution to write the pipeline data to DynamoDB in OVERWRITE/UPSERT/INSERT ONLY mode.</span></p>
        <h1 class="c24 c6" id="h.fl0jlrv779n5"><span class="c15 c17">Prerequisites</span></h1>
        <p class="c0 c4"><span class="c11"></span></p>
        <ol class="c44 lst-kix_doqib9utnpku-0 start" start="1">
            <li class="c0 c6 c28 li-bullet-0"><span class="c11">IAM role should be attached to the EMR cluster and IWX Edgenode that has access write/read access to DynamoDB tables</span></li>
            <li class="c0 c28 c6 li-bullet-0"><span class="c11">Preferably create DynamoDB tables manually from AWS console/cli as there are multiple options/configs available</span></li>
        </ol>
        <p class="c0 c6 c46">
            <span class="c18 c29">
                <a
                    class="c34"
                    href="https://www.google.com/url?q=https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html%23API_CreateTable_RequestSyntax&amp;sa=D&amp;source=editors&amp;ust=1713324872105522&amp;usg=AOvVaw3ReHtCv3hzBnahqgE88Aiz"
                >
                    https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestSyntax
                </a>
            </span>
        </p>
        <p class="c0 c6 c46"><span class="c11">For the POC we have just included basic options as we are not sure which are the options used by Aflac.</span></p>
        <h1 class="c24 c6" id="h.933sr8ijferw"><span class="c15 c17">Step 1: Create Pipeline Extension</span></h1>
        <p class="c0 c6"><span class="c11">Download the jars from the below link and upload them as shown below</span></p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span class="c26">Admin &gt; Extensions &gt; Pipeline Extension &gt; Create pipeline Extension</span>
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 82.67px;">
                <img alt="" src="images/image3.png" style="width: 624px; height: 82.67px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 783.92px; height: 543.5px;">
                <img alt="" src="images/image4.png" style="width: 783.92px; height: 543.5px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <p class="c0 c4"><span class="c1"></span></p>
        <h1 class="c24 c6" id="h.1us7yj3klfnf"><span class="c15 c17">Step 2: Add Pipeline Extension to Domain</span></h1>
        <p class="c0 c4"><span class="c1"></span></p>
        <p class="c0 c6">
            <span class="c18">Go to</span><span class="c13">&nbsp;</span><span class="c26">Manage Domains</span><span class="c13">&nbsp;&gt; </span><span class="c26">Edit the domain with the pipeline</span>
            <span class="c13">&nbsp;&gt; </span><span class="c26">click on manage artifacts</span><span class="c13">&nbsp;&gt; </span><span class="c15 c26 c31">Add the pipeline extension to the domain</span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 92px;">
                <img alt="" src="images/image7.png" style="width: 624px; height: 92px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <h1 class="c24 c6" id="h.x5175kuy9upu"><span class="c15 c17">Step 3: Configure the Custom Target using the Pipeline Extension</span></h1>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6">
            <span style="overflow: hidden; display: inline-block; margin: 0px 0px; border: 0px solid #000000; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px); width: 624px; height: 352px;">
                <img alt="" src="images/image2.png" style="width: 624px; height: 352px; margin-left: 0px; margin-top: 0px; transform: rotate(0rad) translateZ(0px); -webkit-transform: rotate(0rad) translateZ(0px);" title="" />
            </span>
        </p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c4"><span class="c8"></span></p>
        <p class="c0 c6"><span class="c35 c31">Available keys to configure</span></p>
        <p class="c0 c4"><span class="c8"></span></p>
        <a id="t.d003a6b3cef8916f837ed93336efcb8a7c3b5f4b"></a><a id="t.1"></a>
        <table class="c43">
            <tr class="c50">
                <td class="c49" colspan="1" rowspan="1">
                    <p class="c23"><span class="c42">KEY</span></p>
                </td>
                <td class="c37" colspan="1" rowspan="1">
                    <p class="c23"><span class="c42">DESCRIPTION</span></p>
                </td>
            </tr>
            <tr class="c5">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">endpoint</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">Pass the dynamodb endpoint</span></p>
                </td>
            </tr>
            <tr class="c22">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">region</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">Region in which DynamoDB table is hosted</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">ddb_table_name</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">DynamoDB table name</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">key_schema</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">Mandatory input to create table in the format {col_name}:{keytype}:{attributetype}</span></p>
                    <p class="c0"><span class="c20 c48">Example:</span><span class="c15 c7">&nbsp;customer_id:HASH:N</span></p>
                    <p class="c0"><span class="c15 c7">Refer for more details:</span></p>
                    <p class="c0">
                        <span class="c29 c41">
                            <a
                                class="c34"
                                href="https://www.google.com/url?q=https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html%23API_CreateTable_RequestSyntax&amp;sa=D&amp;source=editors&amp;ust=1713324872110690&amp;usg=AOvVaw3tfTHQyIGNbRfgwOPjcFM4"
                            >
                                https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestSyntax
                            </a>
                        </span>
                    </p>
                    <p class="c0"><span class="c15 c7">We can provide both partition key and sort key details here.</span></p>
                    <p class="c0"><span class="c15 c7">Example: partition_key:HASH:S,sort_key:RANGE:N</span></p>
                    <p class="c0"><span class="c40">You can use the combination of both as primary key or provide only partition key as primary key</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">ReadCapacityUnits</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">The maximum number of strongly consistent reads consumed per second before DynamoDB returns a ThrottlingException. &nbsp;Default: 10</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">WriteCapacityUnits</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">The maximum number of writes consumed per second before DynamoDB returns a ThrottlingException. Default: 10</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c7">mode</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">overwrite &rarr; Table will be truncated and overwritten</span></p>
                    <p class="c0"><span class="c7 c15">insert &rarr; If there are only inserts then this mode is preferred to be used</span></p>
                    <p class="c0"><span class="c15 c7">upsert &rarr; If there are updates as well as inserts then one can use upsert mode.</span></p>
                    <p class="c0"><span class="c15 c7">Default: overwrite</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">table_exists</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">true/false</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">writeBatchSize</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">Number of items to send per call to DynamoDB BatchWriteItem. Default 25.</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">targetCapacity</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">Fraction of provisioned write capacity on the table to consume for writing or updating. Default 1 (i.e. 100% capacity).</span></p>
                </td>
            </tr>
            <tr class="c19">
                <td class="c9" colspan="1" rowspan="1">
                    <p class="c0"><span class="c15 c7">throughput</span></p>
                </td>
                <td class="c12" colspan="1" rowspan="1">
                    <p class="c0">
                        <span class="c15 c7">The desired write throughput to use. It overwrites any calculation used by the package. It is intended to be used with tables that are on-demand. Defaults to 100 for on-demand.</span>
                    </p>
                </td>
            </tr>
        </table>
        <p class="c0 c4"><span class="c1"></span></p>
        <h1 class="c24 c6" id="h.7pecyi11m0o9"><span class="c15 c17">Notes:</span></h1>
        <p class="c0 c4"><span class="c14 c13"></span></p>
        <p class="c0 c6"><span class="c45">Jars to upload can be found here: </span></p>
        <p class="c0 c4"><span class="c3"></span></p>
        <p class="c0 c6">
            <span class="c27 c13">
                <a class="c34" href="https://www.google.com/url?q=https://infoworks-releases.s3.amazonaws.com/DynamoDB.zip&amp;sa=D&amp;source=editors&amp;ust=1713324872115629&amp;usg=AOvVaw3gSEw_b79SW3ly_-scNViT">
                    https://infoworks-releases.s3.amazonaws.com/DynamoDB.zip
                </a>
            </span>
        </p>
        <p class="c0 c4"><span class="c3"></span></p>
        <p class="c0 c4"><span class="c3"></span></p>
    </body>
</html>
