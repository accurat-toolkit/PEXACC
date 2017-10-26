#!/usr/bin/perl -w

use strict;
use warnings;
use pexacc2conf; 

sub readClusterIPs( $ );

if ( scalar( @ARGV ) != 1 ) {
	die( "initcluster.pl <cluster.info file>\n" );
}

my( $pexaccconf ) = pexacc2conf->new( {} );
my( %machips ) = readClusterIPs( $ARGV[0] );
my( $workingdir ) = $pexaccconf->{"PEXACCWORKINGDIR"};
my( $tmpdir ) = $pexaccconf->{"TMPDIR"};
my( $newdictdir ) = $pexaccconf->{"GIZAPPNEWDICTDIR"};

#Local cleanup
qx/rm -fv ${workingdir}\/*.in 1>&2/;
qx/rm -fv ${workingdir}\/*.out 1>&2/;
qx/rm -fv ${workingdir}\/*.ready 1>&2/;
qx/rm -fv ${tmpdir}\/* 1>&2/;
qx/rm -fv ${newdictdir}\/* 1>&2/;
qx/killall pdataworker.pl/;

print( STDERR "\nNEFERTITI:\n" );
qx/ps rU rion/;
print( STDERR "\n" );


#Remote cleanup
foreach my $ip ( keys( %machips ) ) {
	next if ( $ip eq $pexaccconf->{"MASTERIP"} || $ip eq "127.0.0.1" );
	
	qx/ssh rion\@${ip} rm -fv ${workingdir}\/*.in 1>&2/;
	qx/ssh rion\@${ip} rm -fv ${workingdir}\/*.out 1>&2/;
	qx/ssh rion\@${ip} rm -fv ${workingdir}\/*.ready 1>&2/;
	qx/ssh rion\@${ip} rm -fv ${tmpdir}\/* 1>&2/;
	qx/ssh rion\@${ip} rm -fv ${newdictdir}\/* 1>&2/;
	qx/ssh rion\@${ip} killall pdataworker.pl/;
	
	print( STDERR "\n" . uc( $machips{$ip} ) . ":\n" );
	qx/ssh rion\@${ip} ps rU rion/;
	print( STDERR "\n" );
}

#pexacc2 ok.
sub readClusterIPs( $ ) {
	my( %cluster ) = ();
	
	open( CLST, "< $_[0]" ) or die( "pexacc2::readClusterIPs: cannot open file '$_[0]' !\n" );
	
	while ( my $line = <CLST> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
	
		next if ( $line =~ /^#/ );
		next if ( $line =~ /^$/ );

		my( $hostname, $ip, $cpuid ) = split( /\s+/, $line );
		$cluster{$ip} = $hostname;
	}

	close( CLST );
	return %cluster;
}
