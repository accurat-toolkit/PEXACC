#!/usr/bin/perl -w

# PEXACC2: a Parallel phrase EXtrActor from Comparable Corpora
# Parallelism level is now on the scoring parallel phrases level.
#
# (C) ICIA 2011, Radu Ion.
#
# ver 0.1, 22.08.2011, Radu ION: added \t separator in document pairs.
# ver 0.2, 04.10.2011, Radu ION: Windows/Unix portable.
# ver 2.0, 11.10.2011, Radu ION: a complete new way of parallel computation. The functionality remains.
# ver 2.01, 31.10.2011, Radu ION: added document pair info to output.
# ver 2.02, 3.11.2011, Radu ION: added selective debugging messages.
# ver 2.5, 19.11.2011, Radu ION: added local file reading for the master node.
# ver 2.6, 19.11.2011, Radu ION: resulting phrase file is now smaller (output phrase pair only if >= OUTPUTTHR)
# ver 2.7, 19.11.2011, Radu ION: fixed a sentence splitting bug (empty sentences were generated).
# ver 3.0, 23.11.2011, Radu ION: heavy modifications: no NFS, scp between master and worker, all files copied on each cluster node.
# ver 3.1, 15.12.2011, Radu ION: added identification of remote worker
# ver 4.0, 15.12.2011, Radu ION: added symmetrical similarity measure

use strict;
use warnings;
use strsim;
use IO::Handle;
use File::Spec;
use File::Path;
use Sys::Hostname;
use Time::HiRes qw( time alarm sleep );
#Modifiy this file to get new config values.
#Or, pass the appropriate values by command line (see below).
use pexacc2conf;

sub readDocPairs( $ );
sub readCluster( $ );
sub readClusterIPs( $ );
sub distributeEvenly( $$$ );
sub runInParallel( $$$$ );
sub extractGIZAPPDict( $$ );
sub cleanTemp();
sub normalizeLang( $ );
sub readCmdLineArguments( @ );
sub readSplitMarkers( $ );
sub splitSentences( $ );
sub readDocument( $$ );
sub splitSentencesAgain( $$ );
sub parallelScorePPairs( $$$$$ );
sub extractPPhrases( $$$$$ );
sub findClusters( $$$$ );
sub tokenizeText( $ );

if ( scalar( @ARGV ) < 2 ) {
	die( "Usage: pexacc2.pl \\
	[--source en] [--target ro] \\
	[--param GIZAPPEXE=/usr/local/giza++-1.0.5/bin/GIZA++] \\
	[--param PLAIN2SNTEXE=/usr/local/giza++-1.0.5/bin/plain2snt.out] \\
	[--param CLUSTERFILE=generate] \\
	[--param SENTRATIO=1.5] \\
	[--param SPLITMODE=chunk] \\
	[--param OUTPUTTHR=0.2] \\
	[--param GIZAPPITERATIONS=3] \\
	--input <--output file from emacc.pl or equivalent> \\
	[--output <output file>]\n" );
}

my( $cmdlineconf ) = readCmdLineArguments( @ARGV );
my( $pexaccconf ) = pexacc2conf->new( $cmdlineconf );

####### CONFIG #############
#PLEASE DO NOT MODIFY HERE!!
#Use the pdataextractconf.pm file so as the changes are also visibile in pdataworker.pl
my( $SRCL ) = $pexaccconf->{"SRCL"};
my( $TRGL ) = $pexaccconf->{"TRGL"};
my( $PEXACCWORKINGDIR ) = $pexaccconf->{"PEXACCWORKINGDIR"};
my( $TMPDIR ) = $pexaccconf->{"TMPDIR"};
my( $DOCPAIRS ) = readDocPairs( $cmdlineconf->{"INPUTFILE"} );
my( $CORPUSNAME ) = $pexaccconf->{"CORPUSNAME"};
my( $OUTFILEBN ) = $pexaccconf->{"OUTFILEBN"};
my( $OUTFILE ) = $pexaccconf->{"OUTFILE"};
my( $CLUSTERFILE ) = $pexaccconf->{"CLUSTERFILE"};
my( $LEARNTDICTFILEST ) = $pexaccconf->{"LEARNTDICTFILEST"};
my( $LEARNTDICTFILETS ) = $pexaccconf->{"LEARNTDICTFILETS"};
my( $GIZAPPNEWDICTDIR ) = $pexaccconf->{"GIZAPPNEWDICTDIR"};
my( $ITERATIONS ) = $pexaccconf->{"GIZAPPITERATIONS"};
my( $DICTTHR ) = $pexaccconf->{"GIZAPPPARALLELTHR"};
my( $GIZAEXE ) = $pexaccconf->{"GIZAPPEXE"};
my( $GIZACFG ) = $pexaccconf->{"GIZAPPCONF"};
my( $PLN2SNT ) = $pexaccconf->{"PLAIN2SNTEXE"};
### Config from old pdataworker
my( %ENMARKERS ) = readSplitMarkers( $pexaccconf->{"ENMARKERSFILE"} );
my( %ROMARKERS ) = readSplitMarkers( $pexaccconf->{"ROMARKERSFILE"} );
my( $SPLITMODE ) = $pexaccconf->{"SPLITMODE"};
my( $OUTPUTTHR ) = $pexaccconf->{"OUTPUTTHR"};
my( $IDENTICALPHRTHR ) = $pexaccconf->{"IDENTICALPHRTHR"};
my( $CLUSTERLIM ) = $pexaccconf->{"CLUSTERLIM"};
my( $SENTRATIO ) = $pexaccconf->{"SENTRATIO"};
my( $DEBUG ) = $pexaccconf->{"DEBUG"};
####### End Config #########

#################### Start main ######################################################################
cleanTemp();

my( $LastOutFileName ) = "";

#GIZA++ iterations
for ( my $it = 1; $it <= $ITERATIONS; $it++ ) {
	my( $dpnumber ) = 0;

	#Collect the results (parallel phrases)
	my( $outfilename ) = $OUTFILEBN . "-${it}.txt";
	
	$LastOutFileName = $outfilename;
	
	open( OUTF, ">", $outfilename ) or die( "pexacc2::main[$it]: cannot open file '$outfilename' because '$!' !\n" );
	binmode( OUTF, ":utf8" );
	OUTF->autoflush( 1 );
	
	#For each document pair, parallel compute the comparability threshold for each pair of phrases.
	foreach my $dp ( @{ $DOCPAIRS } ) {
		$dpnumber++;
		
		print( STDERR "pexacc2::main[$it]: processing document pair no. $dpnumber out of " . scalar( @{ $DOCPAIRS } ) . " document pairs.\n" );
		
		my( $srcd, $trgd ) = @{ $dp };
		
		extractPPhrases( $srcd, $trgd, *OUTF{"IO"}, $dpnumber, $it );
	} #end all document pairs.

	close( OUTF );
	
	#3. GIZA++ training and new dictionary generation
	extractGIZAPPDict( $it, $outfilename ) if ( $it + 1 <= $ITERATIONS );
} #end GIZA++ interations.

#Copy the last output file in the designated config file.
if ( defined( $OUTFILE ) && $OUTFILE ne "" ) {
	pexacc2conf::portableCopyFile2File( $LastOutFileName, $OUTFILE );
}
##################### End new main ###################################################################

#Doing general clean-up from a previous run so as to start clean.
#pexacc2 ok.
sub cleanTemp() {
	pexacc2conf::portableRemoveFileFromDir( $PEXACCWORKINGDIR, "*.in" );
	pexacc2conf::portableRemoveFileFromDir( $PEXACCWORKINGDIR, "*.ready" );
	pexacc2conf::portableRemoveFileFromDir( $PEXACCWORKINGDIR, "*.out" );
	pexacc2conf::portableRemoveAllFilesFromDir( $GIZAPPNEWDICTDIR );
	pexacc2conf::portableRemoveAllFilesFromDir( $TMPDIR );
}

#pexacc2 ok.
sub readDocPairs( $ ) {
	my( @pairs ) = ();
	my( $lcnt ) = 0;
		
	open( DL, "< $_[0]" ) or die( "pexacc2::readDocList: cannot open file $_[0] !\n" );
	binmode( DL, ":utf8" );
	
	while ( my $line = <DL> ) {
		$lcnt++;
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
		
		my( $srcd, $trgd, $score ) = split( /\t+/, $line );
		
		do {
			warn( "pexacc2::readDocList: not defined source document @ line $lcnt !\n" );
			next;
		}
		if ( ! defined( $srcd ) || $srcd eq "" || ( ! -f( $srcd ) ) );

		do {
			warn( "pexacc2::readDocList: not defined target document @ line $lcnt !\n" );
			next;
		}
		if ( ! defined( $trgd ) || $trgd eq "" || ( ! -f( $trgd ) ) );
		
		push( @pairs, [ $srcd, $trgd ] );
	}
	
	close( DL );
	return \@pairs;
}

#pexacc2 ok.
sub readCluster( $ ) {
	my( %cluster ) = ();

	open( CLST, "< $_[0]" ) or die( "pexacc2::readCluster: cannot open file '$_[0]' !\n" );

	while ( my $line = <CLST> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;

		next if ( $line =~ /^#/ );
		next if ( $line =~ /^$/ );

		my( $hostname, $ip, $cpuid ) = split( /\s+/, $line );

		$cluster{$hostname . "." . $cpuid} = $ip;
	}

	close( CLST );
	return %cluster;
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

#pexacc2 ok.
sub extractGIZAPPDict( $$ ) {
	my( $it, $pphrfile ) = @_;
	#Phrases that are the same (both in English for instance), are not to be considered in GIZA++ training.
	#This is the string similarity threshold.
	my( $IDENTICALPHRTHR ) = 0.95;
	#If to GIZA++ reapeating pairs of phrases.
	#Put 1 if you want to skip repeating pairs of phrases.
	my( $DONOTOUTPUTREPEATPHR ) = 0;
	
	#1. Read the output of pexacc2.pl
	my( @corpus ) = ();
	my( $srcsent, $trgsent, $score ) = ( "", "", 0 );
	my( $lcnt ) = 0;

	open( TXT, "< $pphrfile" ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$pphrfile' !\n" );
	binmode( TXT, ":utf8" );

	while ( my $line = <TXT> ) {
		$lcnt++;
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
	
		if ( $line eq "" ) {
			my( $tstsrcs ) = $srcsent;
			my( $tsttrgs ) = $trgsent;
		
			$tstsrcs =~ s/[^[:alpha:]]//g;
			$tsttrgs =~ s/[^[:alpha:]]//g;
			$tstsrcs = lc( $tstsrcs );
			$tsttrgs = lc( $tsttrgs );
		
			#Only if the two fragments are not identical.
			if ( strsim::similarity( $tstsrcs, $tsttrgs ) < $IDENTICALPHRTHR ) {
				#Only if the parallel threshold exceeds the limit.
				if ( $score >= $DICTTHR ) {
					push( @corpus, [ $srcsent, $trgsent, $score ] );
				}
			}
                
			$srcsent = "";
			$trgsent = "";
			$score = 0;
		}
		else {
			if ( $srcsent eq "" ) {
				$srcsent = $line;
			}
			elsif ( $trgsent eq "" ) {
				$trgsent = $line;
			}
			elsif ( $score == 0 && $line =~ /^[0-9]+(?:\.[0-9]+)?$/ ) {
				$score = $line;
			}
			else {
				warn( "pexacc2::extractGIZAPPDict[$it]: line format error in '$line' @ $lcnt !\n" );
			}
		}
	}

	close( TXT );
	
	my( @deletethesefiles ) = ();

	#2. Write GIZA++ text files...
	my( $gizasrctxtbn ) = $OUTFILEBN . "-${it}_${SRCL}";
	my( $gizasrctxtfile ) = $gizasrctxtbn . ".txt";
	my( $gizatrgtxtbn ) = $OUTFILEBN . "-${it}_${TRGL}";
	my( $gizatrgtxtfile ) = $gizatrgtxtbn . ".txt";

	push( @deletethesefiles, $gizasrctxtfile );
	push( @deletethesefiles, $gizatrgtxtfile );
	
	open( SRC, ">", $gizasrctxtfile ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$gizasrctxtfile' !\n" );
	open( TRG, ">", $gizatrgtxtfile ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$gizatrgtxtfile' !\n" );
	binmode( SRC, ":utf8" );
	binmode( TRG, ":utf8" );
		
	my( %alreadyout ) = ();

	foreach my $ct ( @corpus ) {
		if ( $DONOTOUTPUTREPEATPHR ) {
			my( $key ) = $ct->[0] . "#" . $ct->[1];
			
			if ( ! exists( $alreadyout{$key} ) ) {
				print( SRC $ct->[0] . "\n" );
				print( TRG $ct->[1] . "\n" );
				
				$alreadyout{$key} = 1;
			}
		}
		else {
			print( SRC $ct->[0] . "\n" );
			print( TRG $ct->[1] . "\n" );
		}
	}
		
	close( TRG );
	close( SRC );
	
	#3. Prepare GIZA++ internal artefacts...
	#3.1 plain2snt.out
	my( $gizacmd1 ) = "$PLN2SNT $gizasrctxtfile $gizatrgtxtfile";
	
	warn( "pexacc2::extractGIZAPPDict[$it]: running `$gizacmd1`\n" )
		if ( $DEBUG );
		
	pexacc2conf::portableVerboseSystem( $gizacmd1 );
	
	my( $gizasrcvocabfile ) = $gizasrctxtbn . ".vcb";
	
	#Check for plain2snt.out artefacts...
	die( "pexacc2::extractGIZAPPDict[$it]: vocabulary file '" . $gizasrcvocabfile . "' does not exist!\n" )
		if ( ! -f( $gizasrcvocabfile ) );
	push( @deletethesefiles, $gizasrcvocabfile );

	my( $gizatrgvocabfile ) = $gizatrgtxtbn . ".vcb";
	
	die( "pexacc2::extractGIZAPPDict[$it]: vocabulary file '" . $gizatrgvocabfile . "' does not exist!\n" )
		if ( ! -f( $gizatrgvocabfile ) );
	push( @deletethesefiles, $gizatrgvocabfile );
	
	#With different versions of GIZA++, this may not be the case!
	my( $gizacorpusfile ) = $gizatrgtxtbn . "_" . $gizasrctxtbn . ".snt";
	
	die( "pexacc2::extractGIZAPPDict[$it]: corpus file '" . $gizacorpusfile . "' does not exist!\n" )
		if ( ! -f( $gizacorpusfile ) );
	push( @deletethesefiles, $gizacorpusfile );
	
	#3.2 write the .gizacfg temp file using the master config...
	my( $gizatempcfgfile ) = $GIZACFG;
	
	$gizatempcfgfile =~ s/\.[^\.]+$//;
	$gizatempcfgfile .= "-temp.gizacfg";
	
	pexacc2conf::portableCopyFile2File( $GIZACFG, $gizatempcfgfile );
	
	my( $gizaprefix ) = "$SRCL-$TRGL-gizapp-$CORPUSNAME-$it";
	
	open( GZCFG, ">>", $gizatempcfgfile ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$gizatempcfgfile' !\n" );
	
	print( GZCFG "l\n" );
	print( GZCFG "o $gizaprefix\n" );
	print( GZCFG "c $gizacorpusfile\n" );
	print( GZCFG "d\n" );
	print( GZCFG "s $gizatrgvocabfile\n" );
	print( GZCFG "t $gizasrcvocabfile\n" );
	print( GZCFG "tc\n" );

	close( GZCFG );
	push( @deletethesefiles, $gizatempcfgfile );
	
	#4. Running GIZA++
	my( $gizacmd2 ) = "$GIZAEXE $gizatempcfgfile";
	
	warn( "pexacc2::extractGIZAPPDict[$it]: running `$gizacmd2`\n" )
		if ( $DEBUG );
		
	#Let's see the GIZA++ output...
	pexacc2conf::portableVerboseSystem( $gizacmd2 );
	
	#5. Writing new dictionary for pdataworker.pl to read in.
	my( $gizanewdict ) = $gizaprefix . ".actual.ti.final";
	
	open( NEWD, "<", $gizanewdict ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$gizanewdict' !\n" );
	binmode( NEWD, ":utf8" );

	#A single dictionary per iteration.
	#The master and the learnt dictionaries will be linearly combined.
	#Source to target...
	my( $newlearntdictfilest ) = $LEARNTDICTFILEST;

	#WARNING: the name of the new dictionary MUST begin with $SRCL-$TRGL-... or $TRGL-$SRCL-...
	open( NEWDD, ">", $newlearntdictfilest ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$newlearntdictfilest' !\n" );
	binmode( NEWDD, ":utf8" );

	while ( my $line = <NEWD> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
		
		my( $srcw, $trgw, $prob ) = split( /\s+/, $line );
		
		next if ( $srcw eq "NULL" || $trgw eq "NULL" );
		
		print( NEWDD $srcw . "\t" . $trgw . "\t" . $prob . "\n" );
	}
	
	close( NEWDD );
	close( NEWD );

	open( NEWD, "<", $gizanewdict ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$gizanewdict' !\n" );
	binmode( NEWD, ":utf8" );

	#Target to source...
	my( $newlearntdictfilets ) = $LEARNTDICTFILETS;

	#WARNING: the name of the new dictionary MUST begin with $SRCL-$TRGL-... or $TRGL-$SRCL-...
	open( NEWDD, ">", $newlearntdictfilets ) or die( "pexacc2::extractGIZAPPDict[$it]: cannot open file '$newlearntdictfilets' !\n" );
	binmode( NEWDD, ":utf8" );

	while ( my $line = <NEWD> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
		
		my( $srcw, $trgw, $prob ) = split( /\s+/, $line );
		
		next if ( $srcw eq "NULL" || $trgw eq "NULL" );
		
		print( NEWDD $trgw . "\t" . $srcw . "\t" . $prob . "\n" );
	}
	
	close( NEWDD );	
	close( NEWD );
	
	push( @deletethesefiles, $gizanewdict );
	
	#6. Clean up...
	foreach my $f ( @deletethesefiles ) {
		pexacc2conf::portableRemoveFile( $f );
	}
	
	pexacc2conf::portableRemoveFile( "${gizaprefix}.*" );
	pexacc2conf::portableRemoveFile( "*.vcb" );
	pexacc2conf::portableRemoveFile( "*.snt" );
	
	#7. Copy the new learnt dictionary to all worker nodes...
	my( %clusterinfo ) = readClusterIPs( $CLUSTERFILE );
	
	foreach my $ip ( keys( %clusterinfo ) ) {
		if ( $ip ne "127.0.0.1" && $ip ne $pexaccconf->{"MASTERIP"} ) {
			pexacc2conf::portableRemoteCopy( $newlearntdictfilest, "rion", $ip, $GIZAPPNEWDICTDIR );
			pexacc2conf::portableRemoteCopy( $newlearntdictfilets, "rion", $ip, $GIZAPPNEWDICTDIR );
		}
	}
} #end extractGIZAPPDict

#pexacc2 ready.
sub readCmdLineArguments( @ ) {
	my( @args ) = @_;
	my( %clconf ) = ();
	my( %allowedparams ) = (
		"GIZAPPEXE" => 1,
		"PLAIN2SNTEXE" => 1,
		"CLUSTERFILE" => 1,
		"SENTRATIO" => 1,
		"SPLITMODE" => 1,
		"OUTPUTTHR" => 1,
		"GIZAPPITERATIONS" => 1
	);
	
	while ( scalar( @args ) > 0 ) {
		my( $opt ) = shift( @args );
		
		SWOPT: {
			$opt eq "--source" and do {
				$clconf{"SRCL"} = normalizeLang( shift( @args ) );
				last;
			};

			$opt eq "--target" and do {
				$clconf{"TRGL"} = normalizeLang( shift( @args ) );
				last;
			};
			
			$opt eq "--param" and do {
				my( $param, $value ) = split( /\s*=\s*/, shift( @args ) );
				
				$param = uc( $param );

				die( "pexacc2::readCmdLineArguments: unknown parameter '$param' !\n" )
					if ( ! exists( $allowedparams{$param} ) );

				$clconf{$param} = $value;
				last;
			};
			
			$opt eq "--input" and do {
				$clconf{"INPUTFILE"} = shift( @args );
				last;
			};
			
			$opt eq "--output" and do {
				$clconf{"OUTFILE"} = shift( @args );
				last;
			};
			
			die( "pexacc2::readCmdLineArguments: unknown option '$opt' !\n" );
		}
	}

	return \%clconf;
}

#pexacc2 ready.
sub normalizeLang( $ ) {
	my( $lang ) = lc( $_[0] );
	my( %accuratlanguages ) = (
		#1
		"romanian" => "ro",
		"rum" => "ro",
		"ron" => "ro",
		"ro" => "ro",
		#2
		"english" => "en",
		"eng" => "en",
		"en" => "en",
		#3
		"estonian" => "et",
		"est" => "et",
		"et" => "et",
		#4
		"german" => "de",
		"ger" => "de",
		"deu" => "de",
		"de" => "de",
		#5
		"greek" => "el",
		"gre" => "el",
		"ell" => "el",
		"el" => "el",
		#6
		"croatian" => "hr",
		"hrv" => "hr",
		"hr" => "hr",
		#7
		"latvian" => "lv",
		"lav" => "lv",
		"lv" => "lv",
		#8
		"lithuanian" => "lt",
		"lit" => "lt",
		"lt" => "lt",
		#9
		"slovenian" => "sl",
		"slv" => "sl",
		"sl" => "sl"
	);
	
	return $accuratlanguages{$lang} if ( exists( $accuratlanguages{$lang} ) );
	die( "pexacc2::normalizeLang: unknown language '$lang' !\n" );
}

############################ Code from pdataworker that I need in PEXACC2 #########################################
#pexacc2 ready.
sub readSplitMarkers( $ ) {
	my( %mark ) = ();

	open( SMK, "< $_[0]" ) or die( "pexacc2::readSplitMarkers : cannot open file \'$_[0]\' !\n" );
	binmode( SMK, ":utf8" );

	while ( my $line = <SMK> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;

		next if ( $line =~ /_/ );
		next if ( $line =~ /^$/ );

		$mark{$line} = 1;
	}
	
	close( SMK );
	return %mark;
}

#pexacc2 ready.
sub splitSentences( $ ) {
	my( $text ) = $_[0];

	#$text =~ s/((?:\p{IsAlpha}|\p{IsDigit})[.?!:]+)(\s*["'<>\(\[\{]?\s*\p{IsUpper})/$1#CUT#$2/g;
	$text =~ s/(.+?(?<![\s\.]\p{IsUpper})(?<![\s\.]\p{IsUpper}[bcdfgjklmnprstvxz])(?<![\s\.]\p{IsUpper}[bcdfgjklmnprstvxz][bcdfgjklmnprstvxz])[\.?!]+)((?=\s*[\p{IsUpper}\[\(\"\']))/$1#CUT#$2/g;
	$text =~ s/(?:\r?\n)+/ #CUT# /g;

	my( @sentences ) = split( /#CUT#/, $text );
	my( @sentences2 ) = ();

	foreach my $s ( @sentences ) {
		$s =~ s/^\s+//;
		$s =~ s/\s+$//;
		
		push( @sentences2, $s ) if ( defined( $s ) && $s ne "" );
	}

	return @sentences2;
}

sub readDocument( $$ ) {
	my( $doctext ) = "";
	my( $markers ) = $_[1];
	
	open( DOC, "< $_[0]" ) or die( "pexacc2::readDocument: cannot open file '$_[0]' !\n" );
	binmode( DOC, ":utf8" );
	
	while ( my $line = <DOC> ) {
		$doctext .= $line;
	}
	
	close( DOC );
	
	my( @sentences ) = splitSentences( $doctext );
	
	if ( $SPLITMODE eq "sent" ) {
		return @sentences;
	}
	elsif ( $SPLITMODE eq "chunk" ) {
		my( @sentenceparts ) = ();
	
		foreach my $s ( @sentences ) {
			push( @sentenceparts, splitSentencesAgain( $s, $markers ) );
		}
	
		return @sentenceparts;
	}
	else {
		die( "pexacc2::readDocument: unknown split mode ! May be 'sent' or 'chunk' !\n" );
	}
}

#pexacc2 ready.
sub splitSentencesAgain( $$ ) {
	my( $sentence, $markers ) = @_;
	
	return () if ( ! defined( $sentence ) || $sentence eq "" );
	
	my( @swords ) = split( /\s+/, $sentence );
	my( @swords2 ) = ();
	my( @sparts ) = ( [] );
	my( @sparts2 ) = ();
	
	foreach my $w ( @swords ) {
		if ( $w !~ /[[:alnum:]]/ ) {
			push( @swords2, $w );
			next;
		}
		
		my( $antepunct ) = ( $w =~ /^([^[:alnum:]]+)/ );
		my( $postpunct ) = ( $w =~ /([^[:alnum:]]+)$/ );
		my( $wnopunct ) = $w;
		
		$wnopunct =~ s/^[^[:alnum:]]+//;
		$wnopunct =~ s/[^[:alnum:]]+$//;
		
		push( @swords2, $antepunct ) if ( defined( $antepunct ) && $antepunct ne "" );
		push( @swords2, $wnopunct ) if ( defined( $wnopunct ) && $wnopunct ne "" );
		push( @swords2, $postpunct ) if ( defined( $postpunct ) && $postpunct ne "" );
	}
	
	my( $lastmarker ) = 0;
	
	foreach my $w ( @swords2 ) {
		if ( exists( $markers->{$w} ) || exists( $markers->{lc( $w )} ) ) {
			if ( ! $lastmarker ) {
				my( @temp ) = ( $w );
			
				push( @sparts, \@temp );
				
			}
			else {
				push( @{ $sparts[$#sparts] }, $w );
			}
			
			$lastmarker = 1;
		}
		else {
			push( @{ $sparts[$#sparts] }, $w );
			$lastmarker = 0;
		}
	} 

	foreach my $sp ( @sparts ) {
		$sp = join( " ", @{ $sp } );
		
		if ( $sp ne "" ) {
			push( @sparts2, $sp );
		}
	}
	
	return ( @sparts2, "#EOS#" );
}

#pexacc2 ok.
sub distributeEvenly( $$$ ) {
	my( $srcphr, $trgphr, $totalcpu ) = @_;
	my( @batchfiles ) = ();
	my( $phrcount ) = 0;
	my( $howmany ) = 0;
	my( $cpucount ) = 1;
	my( $crtbfile ) = File::Spec->catfile( $TMPDIR, "$SRCL-$TRGL-phrase-batch-$cpucount.pp" );
	
	foreach my $s ( @{ $srcphr } ) {
		next if ( $s eq "#EOS#" );
		
		foreach my $t ( @{ $trgphr } ) {
			next if ( $t eq "#EOS#" );
			
			$howmany++;
		}
	}
	
	print( STDERR "pexacc2::distributeEvenly: writing to file '$crtbfile'...\n" )
		if ( $DEBUG );
	
	open( PAIRS, ">", $crtbfile ) or die( "pexacc2::distributeEvenly: cannot open file '$crtbfile' !\n" );
	binmode( PAIRS, ":utf8" );
	
	for ( my $i = 0; $i < scalar( @{ $srcphr } ); $i++ ) {
		my( $sp ) = $srcphr->[$i];
		
		next if ( $sp eq "#EOS#" );
		next if ( ! defined( $sp ) || $sp eq "" );
		
		for ( my $j = 0; $j < scalar( @{ $trgphr } ); $j++ ) {
			my( $tp ) = $trgphr->[$j];
			
			next if ( $tp eq "#EOS#" );
			next if ( ! defined( $tp ) || $tp eq "" );
			
			$phrcount++;
			
			if ( $phrcount >= $howmany / $totalcpu && $cpucount < $totalcpu ) {
				push( @batchfiles, [ $crtbfile, $phrcount ] );
				close( PAIRS );
				
				$phrcount = 1;
				$cpucount++;
				$crtbfile = File::Spec->catfile( $TMPDIR, "$SRCL-$TRGL-phrase-batch-$cpucount.pp" );
				
				print( STDERR "pexacc2::distributeEvenly: writing to file '$crtbfile'...\n" )
					if ( $DEBUG );
				
				open( PAIRS, ">", $crtbfile ) or die( "pexacc2::distributeEvenly: cannot open file '$crtbfile' !\n" );
				binmode( PAIRS, ":utf8" );
			}

			print( PAIRS $i . "#SPLIT-HERE#" . $j . "#SPLIT-HERE#" . $sp . "#SPLIT-HERE#" . $tp . "\n" );
		}
	}
	
	push( @batchfiles, [ $crtbfile, $phrcount ] );
	close( PAIRS );
	
	return @batchfiles;
}

#pexacc2 ready.
sub runInParallel( $$$$ ) {
	my( $ppbatchfile ) = $_[0];
	my( $mach, $machip ) = ( $_[1], $_[2] );
	my( $inoutbasename ) = $_[3];
	my( $thishostname ) = hostname();
	my( $infile ) =  File::Spec->catfile( $PEXACCWORKINGDIR, $inoutbasename . ".in" );
	
	#Identify the remote status of the pdataworker process
	if ( $mach !~ /^${thishostname}/ ) {
		$pexaccconf->{"REMOTEWORKER"} = 1;
	}
	else {
		$pexaccconf->{"REMOTEWORKER"} = 0;
	}	
	
	#1. Write output for worker...
	open( IN, "> " . $infile ) or die( "pexacc2::runInParallel: cannot open file " . $infile . " !\n" );
	binmode( IN, ":utf8" );

	#Random code
	print( IN $inoutbasename . "\n" );
	
	#Print all parameters (including those from the command line):
	foreach my $p ( keys( %{ $pexaccconf } ) ) {
		print( IN "--param $p" . "=" . $pexaccconf->{$p} . "\n" );
	}
	
	open( PAIRS, "<", $ppbatchfile ) or die( "pexacc2::runInParallel: cannot open batch file '$ppbatchfile' !\n" );
	binmode( PAIRS, ":utf8" );

	while ( my $line = <PAIRS> ) {
		print( IN $line );
	}
	
	close( PAIRS );
	close( IN );

	#Do work.
	if ( $mach !~ /^${thishostname}/ ) {
		#1. Copy the work input file to the worker in $PEXACCWORKINGDIR
		pexacc2conf::portableRemoteCopy( $infile, "rion", $machip, $PEXACCWORKINGDIR );
		#2. Linux only. Fork and detach. $infile must be locally available!!
		system( "ssh rion\@${machip} '.\/pdataworker.pl ${infile}' &" );
	}
	else {
		warn( "pexacc2::runInParallel: executing on localhost ...\n" )
			if ( $DEBUG );
		#If we are on this host, no ssh is needed.
		pexacc2conf::portableForkAndDetach( "perl pdataworker.pl ${infile}" );
	}
}

#pexacc2 ready.
sub parallelScorePPairs( $$$$$ ) {
	my( $srcsent, $trgsent, $outfileh, $dpnumber, $it ) = @_;
	my( %clusterinfo ) = readCluster( $CLUSTERFILE );
	my( $totalcpuno ) = scalar( keys( %clusterinfo ) );
	my( @cluster ) = keys( %clusterinfo );	

	#Distribute the work for this document pair...
	my( %checkoutfiles ) = ();
	my( @phrasepairsfiles ) = distributeEvenly( $srcsent, $trgsent, $totalcpuno );

	for ( my $cnt = 0; $cnt < scalar( @phrasepairsfiles ); $cnt++ ) {
		my( $ppbatchfile, $loadfactor ) = @{ $phrasepairsfiles[$cnt] };

		#Here we run computing on the cluster ...
		my( $mach ) = shift( @cluster );
		my( $inoutfbname ) =
			$loadfactor . "-" .
			$cnt . "-" .
			$dpnumber . "-" .
			$mach . "-" .
			$SRCL . "-" .
			$TRGL . "-" .
			$CORPUSNAME;

		print( STDERR "pexacc2::parallelScorePPairs[it=$it,dp=$dpnumber]: starting worker '$inoutfbname' on machine '$mach' ...\n" )
			if ( $DEBUG );
			
		$checkoutfiles{$inoutfbname} = 1;
		runInParallel( $ppbatchfile, $mach, $clusterinfo{$mach}, $inoutfbname );
		
		#Free some memory
		pexacc2conf::portableRemoveFile( $ppbatchfile );
	} #end all processes

	#Here we must do a simple concatenation of the results...
	#Wait for partial results...
	my( $tries ) = 0;
	my( $checkoutno ) = scalar( keys( %checkoutfiles ) );
	
	while ( scalar( keys( %checkoutfiles ) ) > 0 ) {
		foreach my $io ( keys( %checkoutfiles ) ) {
			my( $infile ) = File::Spec->catfile( $PEXACCWORKINGDIR, $io . ".in" );
			my( $readyfile ) = File::Spec->catfile( $PEXACCWORKINGDIR, $io . ".ready" );
			my( $pdatafile ) = File::Spec->catfile( $PEXACCWORKINGDIR, $io . ".out" );

			if ( -f( $readyfile ) ) {
				open( PDAT, "<", $pdatafile ) or die( "pexacc2::parallelScorePPairs[it=$it,dp=$dpnumber]: cannot open file '$pdatafile' because '$!' !\n" );
				binmode( PDAT, ":utf8" );

				while ( my $line = <PDAT> ) {
					$line =~ s/^\s+//;
					$line =~ s/\s+$//;
					
					my( $i, $j, $ss, $ts, $pprob ) = split( /#SPLIT-HERE#/, $line );
					
					print( $outfileh $line . "\n" ) if ( $pprob >= $OUTPUTTHR );
				}

				close( PDAT );
			
				print( STDERR "pexacc2::parallelScorePPairs[it=$it,dp=$dpnumber]: " . ( 1 - scalar( keys( %checkoutfiles ) ) / $checkoutno ) . "%" )
					if ( $DEBUG );

				pexacc2conf::portableRemoveFile( $infile );
				pexacc2conf::portableRemoveFile( $readyfile );
				pexacc2conf::portableRemoveFile( $pdatafile );

				delete( $checkoutfiles{$io} );
			}
		}
		
		$tries++;
		
		#Sometimes files are deleted but PEXACC stucks in this loop.
		#This is for breaking.
		if ( $tries >= 1000 ) {
			my( @filesin ) = pexacc2conf::portableListFiles( File::Spec->catfile( $PEXACCWORKINGDIR, "*.in" ) );
			my( @filesout ) = pexacc2conf::portableListFiles( File::Spec->catfile( $PEXACCWORKINGDIR, "*.out" ) );
			my( @filesready ) = pexacc2conf::portableListFiles( File::Spec->catfile( $PEXACCWORKINGDIR, "*.ready" ) );
			
			if ( scalar( @filesin ) == 0 && scalar( @filesout ) == 0 ) {
				print( STDERR "pexacc2::parallelScorePPairs[it=$it,dp=$dpnumber]: force break from loop.\n" )
					if ( 1 );

				last;
			}
			
			$tries = 0;
		}
		
		$tries++;
	} #end all partial results.
} #end parallelScorePPairs.

#pexacc2 ready.
sub extractPPhrases( $$$$$ ) {
	my( $srcd, $trgd, $outfile, $dpnumber, $it ) = @_;
	my( @srcsent ) = readDocument( $srcd, \%ENMARKERS );
	my( @trgsent ) = readDocument( $trgd, \%ROMARKERS );
	my( $pphrasesfile ) = File::Spec->catfile( $TMPDIR, "$it-$SRCL-$TRGL-$dpnumber-$CORPUSNAME-extracted.pdat" );
	
	open( PDEX, ">", $pphrasesfile ) or die( "pexacc2::extractPPhrases[it=$it,dp=$dpnumber]: cannot open file '$pphrasesfile' for writing!\n" );
	binmode( PDEX, ":utf8" );
	
	parallelScorePPairs( \@srcsent, \@trgsent, *PDEX{"IO"}, $dpnumber, $it );
	
	close( PDEX );

	my( %mappedtxtunits ) = ();
	
	open( PDAT, "<", $pphrasesfile ) or die( "pexacc2::extractPPhrases[$it,dp=$dpnumber]: cannot open file '$pphrasesfile' for reading!\n" );
	binmode( PDAT, ":utf8" );

	while ( my $line = <PDAT> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
		
		my( $i, $j, $ss, $ts, $pprob ) = split( /#SPLIT-HERE#/, $line );
		
		SWPHRTYPE: {
			( $SPLITMODE eq "sent" || $CLUSTERLIM == 0 ) and do {
				print( $outfile $ss . "\n" . $ts . "\n" . $pprob . "\n\n" )
					if (
						$pprob >= $OUTPUTTHR &&
						strsim::similarity( $ss, $ts ) < $IDENTICALPHRTHR
					);
					
				last;
			};
				
			( $SPLITMODE eq "chunk" && $CLUSTERLIM > 0 ) and do {
				if ( $pprob >= $OUTPUTTHR ) {
					if ( ! exists( $mappedtxtunits{$i} ) ) {
						$mappedtxtunits{$i} = { $j => $pprob };
					}
					elsif ( ! exists( $mappedtxtunits{$i}->{$j} ) ) {
						$mappedtxtunits{$i}->{$j} = $pprob;
					}
				}
			
				last;
			};
		} #end splitmode		
	} #end all phrase pairs.
	
	close( PDAT );
	
	if ( $SPLITMODE eq "chunk" && $CLUSTERLIM > 0 ) {
		my( $previ ) = -1;
		my( @crticluster ) = ();
		my( @crtjclusters ) = ();
	
		#Search for adjacent mappings... and align.
		foreach my $i ( sort { $a <=> $b } keys( %mappedtxtunits ) ) {
			if ( $previ >= 0 ) {
				my( $subeos ) = sub {
					my( $eos ) = 0;
					
					for ( my $k = $previ; $k <= $i; $k++ ) {
						$eos = 1 if ( $srcsent[$k] eq "#EOS#" );
					}
					
					$eos;
				};
					
				#If the phrases are near each other in the source language...
				if ( $i - $previ <= $CLUSTERLIM && ! $subeos->() ) {
					my( $ijshash ) = { %{ $mappedtxtunits{$i} } };
					my( $previjshash ) = $crtjclusters[$#crtjclusters];
					my( $connect ) = 0;
						
					LASTJ:
					foreach my $j ( keys( %{ $ijshash } ) ) {
						foreach my $pj ( keys( %{ $previjshash } ) ) {
							if ( abs( $j - $pj ) <= $CLUSTERLIM ) {
								$connect = 1;
								last LASTJ;
							}
						}
					}
						
					if ( $connect ) {
						push( @crticluster, $i );
						push( @crtjclusters, $ijshash );
						$previ = $i;							
						next;
					}
				} #end if possible cluster
					
				#Read clusters and align.
				my( @solutions ) = ();
					
				findClusters( 0, \@crtjclusters, [], \@solutions );
					
				#Remove j clusters if they are included in one another...
				for ( my $j1 = 0; $j1 < scalar( @solutions ) - 1; $j1++ ) {
					next if ( $solutions[$j1] eq "" );
					
					my( @jc1 ) = sort { $a <=> $b } @{ $solutions[$j1] };
					
					for ( my $j2 = $j1 + 1; $j2 < scalar( @solutions ); $j2++ ) {
						next if ( $solutions[$j2] eq "" );
						
						my( @jc2 ) = sort { $a <=> $b } @{ $solutions[$j2] };
						
						#Aici am ramas!!
						#jc1 included in jc2
						if ( $jc1[0] >= $jc2[0] && $jc1[$#jc1] <= $jc2[$#jc2] ) {
							$solutions[$j1] = "";
						}
						#jc2 included in jc1
						elsif ( $jc2[0] >= $jc1[0] && $jc2[$#jc2] <= $jc1[$#jc1] ) {
							$solutions[$j2] = "";
						}
					}
				}
					
				foreach my $jcl ( @solutions ) {
					#This was an included cluster...
					next if ( $jcl eq "" );
					
					my( @crtjcluster ) = sort { $a <=> $b } @{ $jcl };
					
					#Print the alignment between crticluster and crtjcluster
					my( @ssarr ) = do {
						my( @temp ) = ();
						
						for ( my $k = $crticluster[0]; $k <= $crticluster[$#crticluster]; $k++ ) {
							push( @temp, split( /\s+/, $srcsent[$k] ) ) if ( $srcsent[$k] ne "#EOS" );
						}
						
						@temp;
					};
					my( @tsarr ) = do {
						my( @temp ) = ();
							
						for ( my $k = $crtjcluster[0]; $k <= $crtjcluster[$#crtjcluster]; $k++ ) {
							if ( $trgsent[$k] eq "#EOS#" ) {
								@temp = ();
								next;
							}
								
							push( @temp, split( /\s+/, $trgsent[$k] ) );
						}
							
						@temp;
					};
					
					my( $ssc ) = 0;
					my( $ppno ) = 0;
					
					foreach my $icn ( @crticluster ) {
						foreach my $jcn ( @crtjcluster ) {
							if ( exists( $mappedtxtunits{$icn}->{$jcn} ) ) {
								$ssc += $mappedtxtunits{$icn}->{$jcn};
								$ppno++;
							}
						}
					} #end compute average of the scores.
					
					$ssc = $ssc / $ppno;
					
					my( $ss ) = join( " ", @ssarr );
					my( $ts ) = join( " ", @tsarr );
					
					if ( $ssc >= $OUTPUTTHR && strsim::similarity( $ss, $ts ) < $IDENTICALPHRTHR ) {
						print( $outfile $ss . "\n" . $ts . "\n" . $ssc . "\n\n" );
					
						#Delete the clusters from the found mappings...
						foreach my $ii ( @crticluster ) {
							foreach my $jj ( @crtjcluster ) {
								delete( $mappedtxtunits{$ii}->{$jj} )
									if ( exists( $mappedtxtunits{$ii} ) && exists( $mappedtxtunits{$ii}->{$jj} ) );
							}
						}
					}
				} #end all clusters.
			} #end if previ >= 0.
					
			@crticluster = ( $i );
			@crtjclusters = ( { %{ $mappedtxtunits{$i} } } );
			$previ = $i;
		} #end i and ajacent phrases alignment
			
		#Map the rest of the phrases.
		foreach my $i ( keys( %mappedtxtunits ) ) {
			my( $ss ) = $srcsent[$i];
			
			foreach my $j ( keys( %{ $mappedtxtunits{$i} } ) ) {
				my( $ts ) = $trgsent[$j];
				my( $ssc ) = $mappedtxtunits{$i}->{$j};
					
				if ( $ssc >= $OUTPUTTHR && strsim::similarity( $ss, $ts ) < $IDENTICALPHRTHR ) {
					print( $outfile $ss . "\n" . $ts . "\n" . $ssc . "\n\n" );
				}
			}
		} #end print the rest of the phrases.
	} #end if SPLITMODE is chunk.
	
	pexacc2conf::portableRemoveFile( $pphrasesfile );
} #end extractPPhrases.

#pexacc2 ready
sub findClusters( $$$$ ) {
	my( $i, $crtjclusters, $crtsol, $solutions ) = @_;
	
	if ( $i >= scalar( @{ $crtjclusters } ) ) {
		push( @{ $solutions }, [ @{ $crtsol } ] );
		return;
	}
	
	foreach my $j ( keys( %{ $crtjclusters->[$i] } ) ) {
		if ( scalar( @{ $crtsol } ) == 0 ) {
			push( @{ $crtsol }, $j );
			findClusters( $i + 1, $crtjclusters, $crtsol, $solutions );
			pop( @{ $crtsol } );
		}
		else {
			my( $addj ) = 0;
			
			foreach my $cj ( @{ $crtsol } ) {
				if ( abs( $j - $cj ) <= $CLUSTERLIM ) {
					$addj = 1;
					last;
				}
			}
			
			if ( $addj ) {
				push( @{ $crtsol }, $j );
				findClusters( $i + 1, $crtjclusters, $crtsol, $solutions );
				pop( @{ $crtsol } );
			}
		}
	} #end all j from current set
}

sub tokenizeText( $ ) {
	my( $text ) = $_[0];
	my( @toktext ) = split( /\s+/, $text );
	my( @finaltoktext ) = ();

	foreach my $t ( @toktext ) {
		next if ( ! defined( $t ) || $t eq "" );

		if ( $t =~ /^(\W+)/ ) {
			push( @finaltoktext, split( //, $1 ) );
			$t =~ s/^\W+//;
		}

		if ( $t =~ /(\W+)$/ ) {
			my( $endpunct ) = $1;

			$t =~ s/\W+$//;
			push( @finaltoktext, $t ) if ( $t ne "" );
			push( @finaltoktext, split( //, $endpunct ) );
		}
		else {
			push( @finaltoktext, $t ) if ( $t ne "" );
		}
	}

	return @finaltoktext;
}
