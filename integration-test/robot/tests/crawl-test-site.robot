*** Settings ***
Documentation	Initiate test site crawl and verify
Library			Process

*** Test Cases ***
Launch test crawl
	Run Process	launch -S -k kafka:9092 uris.tocrawl.fc http://crawl-test-site.webarchive.org.uk	shell=yes

