*** Settings ***
Documentation	Initiate test site crawl and verify
Library			Process
Library         RequestsLibrary
Library			XML


*** Variables ***
${user}=    "heritrix"
${pass}=    "heritrix"
${url}=   https://heritrix:8443/engine/job/frequent
&{headers}=   content-type=application/xml   accept=application/xml


*** Test Cases ***
Wait For Crawler To Be Ready
	Wait Until Keyword Succeeds	3 min	10 sec	Check Crawler Status	EMPTY

Launch first test crawl
	Launch Crawl	http://crawl-test-site.webarchive.org.uk

Launch second test crawl
	Launch Crawl	http://acid.matkelly.com/

Wait For Crawl To Run
	Wait Until Keyword Succeeds	1 min	5 sec	Check Crawler Status	RUNNING

Wait For Crawl To Finish
	Wait Until Keyword Succeeds	5 min	10 sec	Check Crawler Status	EMPTY
	Log	Crawl has completed.


*** Keywords ***
Check Crawler Status
	[Arguments]    ${expected}
        Log	Checking crawler status...
	${user_pass}=   Evaluate   (${user}, ${pass},)
	Create Digest Session	h3session	${url}    auth=${user_pass}   headers=${headers}		verify=${false}	disable_warnings=1
	${resp}=    Get On Session    h3session	${url}    headers=${headers}		verify=${false}
	# Log the response, to help debugging: 
	Log	${resp.text}
	${status}=	Get Element Text	${resp.text}	crawlControllerState
	Log	${status}
	Should Match	${status}    ${expected}

Launch Crawl
	[Arguments]	${url}
        Log	Launching crawl of ${url}...
	${result}=	Run Process	submit -k kafka:9092 -S -R fc.tocrawl ${url}	shell=yes
	Should Not Contain	${result.stderr}	Traceback
	Log	${result.stdout}
	Log	${result.stderr}
	Should Be Equal As Integers	${result.rc}	0

