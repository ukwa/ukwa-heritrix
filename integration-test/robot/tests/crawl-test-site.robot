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
	Wait Until Keyword Succeeds	5 min	10 sec	Check Crawler Status	EMPTY

Launch first test crawl
	${result}=	Run Process	submit -k kafka:9092 -S -R fc.tocrawl -p 2 http://crawl-test-site.webarchive.org.uk	shell=yes
	Should Not Contain	${result.stderr}	Traceback
	Log	${result.stdout}
	Log	${result.stderr}
	Should Be Equal As Integers	${result.rc}	0

Launch second test crawl
	${result}=	Run Process	submit -k kafka:9092 -S -R fc.tocrawl http://acid.matkelly.com/	shell=yes
	Should Not Contain	${result.stderr}	Traceback
	Log	${result.stdout}
	Log	${result.stderr}
	Should Be Equal As Integers	${result.rc}	0

Wait For Crawl To Run
	Wait Until Keyword Succeeds	1 min	5 sec	Check Crawler Status	RUNNING

Wait For Crawl To Finish
	Wait Until Keyword Succeeds	5 min	10 sec	Check Crawler Status	EMPTY
	Log To Console	Crawl has completed.


*** Keywords ***
Check Crawler Status
	[Arguments]    ${expected}
    ${user_pass}=   Evaluate   (${user}, ${pass},)
    Create Digest Session	h3session	${url}    auth=${user_pass}   headers=${headers}		verify=${false}	disable_warnings=1
    ${resp}=    Get On Session    h3session	${url}    headers=${headers}		verify=${false}
    ${status}=	Get Element Text	${resp.text}	crawlControllerState
    Log To Console	${status}
	Should Match	${status}    ${expected}