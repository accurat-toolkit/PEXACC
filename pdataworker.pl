#!/usr/bin/perl -w 

#pdataworker.pl is the computing part of PEXACC.
#
# (C) ICIA 2011, Radu ION.

#ver 1.8, 08.08.2011, Radu ION: added word normalization (for string similarity) as a function of languages.
#ver 1.9, 09.08.2011, Radu ION: modified scoreSentPair() scoring.
#ver 2.0, 10.08.2011, Radu ION: added lemmatization.
#ver 2.1, 23.08.2011, Radu ION: added user support.
#ver 2.2, 12.09.2011, Radu ION: added phrase alignment when consecutive phrases map.
#ver 2.3, 22.09.2011, Radu ION: added a tokenization function.
#ver 2.4, 04.10.2011, Radu ION: Windows/Unix portable.
#ver 2.5, 04.10.2011, Radu ION: Bug (Marcis): phrases which do not have the same numbers, should not align.
#ver 3.0, 11.10.2011, Radu ION: a complete new way of parallel computing. Functionality remains at ver 2.5.
#ver 3.01, 3.11.2011, Radu ION: added selective debug messages.
#ver 3.1, 19.11.2011, Radu ION: output a phrase pair only if its prob >= OUTPUTTHR (keep NFS traffic low)
#ver 4.0, 06.12.2011, Radu ION: no NFS drive, scp outfiles back to the master.
#ver 4.1, 15.12.2011, Radu ION: added filtering such that 10. -> 10) would not be aligned anymore.
#ver 4.41, 15.12.2011, Radu ION: fixed portableRemoteCopy.
#ver 5.0, 15.12.2011, Radu ION: PEXACC similarity measure is now symmetrical.
#ver 5.1, 18.12.2011, Radu ION: added Romanian diacrtics handling.
#ver 5.2, 18.12.2011, Radu ION: modified PEXACC similarity measure to have lists of content words only.

use strict;
use warnings;
use strsim;
use IO::Handle;
use Time::HiRes qw( time alarm sleep );
use File::Spec;
use File::Path;

sub portableRemoteCopy( $$$$ );
sub portableRemoveFile( $ );
sub portableCopyFileToDir( $$ );
sub scorePPhrases( $$ );
sub tokenizeText( $ );
sub readGIZAPPDict( $$$$ );
sub normalizeWord( $$ );
sub pairInDict( $$$ );
sub combineDictionaries( $$ );
sub readStopWordsList( $ );
sub scoreSentPair( $$ );
sub scoreSentPairLP( $$$ );
sub isNamedEntity( $ );
sub readSplitMarkers( $ );
sub readInputParams( $ );
sub lemmatizeWord( $$ );
sub readInflectionList( $ );

sub featStartOrEndWithTranslations( $$$ );
sub featAlignNotScrambled( $$$ );
sub boolHaveSameNumbers( $$ );
sub featHaveFinalPunct( $$ );

if ( scalar( @ARGV ) != 1 ) {
	die( "pdataworker.pl <file from pexacc2.pl>\n" );
}

my( $outfilebasename, $pexaccconf ) = readInputParams( $ARGV[0] );

####### CONFIG #############
#Please do not modify here.
#Use pdataextractconf.pm so that the same data is available for the master process, pdataextract-p.pl
my( $DEBUG ) = $pexaccconf->{"DEBUG"};
my( $SRCL ) = $pexaccconf->{"SRCL"};
my( $TRGL ) = $pexaccconf->{"TRGL"};
my( $GIZAPPTHR ) = $pexaccconf->{"GIZAPPTHR"};
my( $NEWGIZAPPTHR ) = $pexaccconf->{"NEWGIZAPPTHR"};
my( $SUREGIZAPPTHR ) = $pexaccconf->{"SUREGIZAPPTHR"};
my( $SENTRATIO ) = $pexaccconf->{"SENTRATIO"};
my( $PEXACCWORKINGDIR ) = $pexaccconf->{"PEXACCWORKINGDIR"};
my( $MASTERIP ) = $pexaccconf->{"MASTERIP"};
my( $TMPDIR ) = $pexaccconf->{"TMPDIR"};

#Try to create the directory.
mkpath( $TMPDIR );

my( $LEMMAS ) = $pexaccconf->{"LEMMAS"};
my( %INFLEN ) = readInflectionList( $pexaccconf->{"INFLENFILE"} );
my( %INFLRO ) = readInflectionList( $pexaccconf->{"INFLROFILE"} );
my( %ENSTOPWORDS ) = readStopWordsList( $pexaccconf->{"ENSTOPWORDSFILE"} );
my( %ROSTOPWORDS ) = readStopWordsList( $pexaccconf->{"ROSTOPWORDSFILE"} );

#Create resource 'objects' with all resources.
my( %ENRORES ) = (
	"enLang" => $SRCL,
	"roLang" => $TRGL,
	"enInflections" => \%INFLEN,
	"roInflections" => \%INFLRO,
	"enStopWords" => \%ENSTOPWORDS,
	"roStopWords" => \%ROSTOPWORDS,
	"enroDict" => {}
);

my( %ROENRES ) = (
	"enLang" => $TRGL,
	"roLang" => $SRCL,
	"enInflections" => \%INFLRO,
	"roInflections" => \%INFLEN,
	"enStopWords" => \%ROSTOPWORDS,
	"roStopWords" => \%ENSTOPWORDS,
	"enroDict" => {}
);

my( $ENRODICTBASE ) = {};
my( $ROENDICTBASE ) = {};

readGIZAPPDict( $pexaccconf->{"DICTFILEST"}, $ENRODICTBASE, $GIZAPPTHR, \%ENRORES );
readGIZAPPDict( $pexaccconf->{"DICTFILETS"}, $ROENDICTBASE, $GIZAPPTHR, \%ROENRES );

#Reading the additional learnt dictionary (always from SRCL to TRGL).
#Strategy to be determined (what happens if we already have a pair of translation equivalents?)
#Determined: linear combination.
my( $DICTWEIGHTMAIN ) = $pexaccconf->{"DICTWEIGHTMAIN"};
my( $DICTWEIGHTLEARNT ) = $pexaccconf->{"DICTWEIGHTLEARNT"};

my( $ENRODICTLEARNT ) = {};
my( $ROENDICTLEARNT ) = {};

if ( -f $pexaccconf->{"LEARNTDICTFILEST"} && -f $pexaccconf->{"LEARNTDICTFILETS"} ) {
	readGIZAPPDict( $pexaccconf->{"LEARNTDICTFILEST"}, $ENRODICTLEARNT, $NEWGIZAPPTHR, \%ENRORES );
	readGIZAPPDict( $pexaccconf->{"LEARNTDICTFILETS"}, $ROENDICTLEARNT, $NEWGIZAPPTHR, \%ROENRES );
}

#Combine dictionaries...
my( $ENRODICT ) = combineDictionaries( $ENRODICTBASE, $ENRODICTLEARNT );
my( $ROENDICT ) = combineDictionaries( $ROENDICTBASE, $ROENDICTLEARNT );

my( $SSTHR ) = $pexaccconf->{"SSTHR"};
my( $REMOTEWORKER ) = $pexaccconf->{"REMOTEWORKER"};
my( $OUTPUTTHR ) = $pexaccconf->{"OUTPUTTHR"};
####### End Config #########

#Update objects with the dictionaries...
$ENRORES{"enroDict"} = $ENRODICT;
$ROENRES{"enroDict"} = $ROENDICT;

#Reopen STDERR to see what's wrong.
my( $errfileL ) = File::Spec->catfile( $TMPDIR, $outfilebasename . ".err" );

open( FERROR, "> $errfileL" ) or die( "pdataworker::main: cannot open FERROR!\n" );
STDERR->fdopen( \*FERROR, 'w' ) or die( "pdataworker::main: cannot reopen STDERR!\n" );
STDERR->autoflush( 1 );
binmode( STDERR, ":utf8" );

my( $outfileL ) = File::Spec->catfile( $TMPDIR, $outfilebasename . ".out" );

open( OUT, "> $outfileL" ) or die( "pdataworker::main: cannot open file '$outfileL' because '$!' !\n" );
binmode( OUT, ":utf8" );
OUT->autoflush( 1 );

scorePPhrases( $ARGV[0], *OUT{"IO"} );

close( OUT );
close( FERROR );

#If result obtained remotely
if ( $REMOTEWORKER ) {
	#Move RESULT on the Master in PEXACCWORKINGDIR
	portableRemoteCopy( $outfileL, "rion", $MASTERIP, $PEXACCWORKINGDIR );
}
#else, simply copy in the proper location
else {
	#Copy RESULT in PEXACCWORKINGDIR
	portableCopyFileToDir( $outfileL, $PEXACCWORKINGDIR );
}

#Clean
portableRemoveFile( $errfileL );
portableRemoveFile( $outfileL );

#Guard against reading incomplete files.
my( $readyfileL ) = File::Spec->catfile( $TMPDIR, $outfilebasename . ".ready" );

open( RDY, "> $readyfileL" ) or die( "pdataworker::main: cannot open file '$readyfileL' !\n" );
close( RDY );

if ( $REMOTEWORKER ) {
	#Move READY onto NFS...
	portableRemoteCopy( $readyfileL, "rion", $MASTERIP, $PEXACCWORKINGDIR );
}
else {
	portableCopyFileToDir( $readyfileL, $PEXACCWORKINGDIR );
}

#Clean
portableRemoveFile( $readyfileL );

########################### from pexacc2conf.pm ###########################################
sub portableRemoteCopy( $$$$ ) {
	my( $localfile, $remoteuser, $remotemachine, $remotedir ) = @_;

	if ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/scp ${localfile} ${remoteuser}\@${remotemachine}:${remotedir} 1>&2/;
		}
		else {
			qx/scp -q ${localfile} ${remoteuser}\@${remotemachine}:${remotedir}/;
		}
	}
	else {
		die( "pexacc2conf::portableRemoteCopy: unsupported operating system '$^O' !\n" );
	}
}

sub portableRemoveFile( $ ) {
	my( $file ) = $_[0];

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`del \/F \/Q ${file}'\n" );
		}
		
		qx/del \/F \/Q ${file}/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/rm -fv ${file} 1>&2/;
		}
		else {
			qx/rm -f ${file}/;
		}
	}
	else {
		die( "pexacc2conf::portableRemoveFile: unsupported operating system '$^O' !\n" );
	}
}

sub portableCopyFileToDir( $$ ) {
	my( $file, $dir ) = @_;

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`copy \/Y ${file} ${dir}\\'\n" );
		}
			
		qx/copy \/Y ${file} ${dir}\\/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/cp -fv ${file} ${dir}\/ 1>&2/;
		}
		else {
			qx/cp -f ${file} ${dir}\//;
		}
	}
	else {
		die( "pexacc2conf::portableCopyFileToDir: unsupported operating system '$^O' !\n" );
	}
}

########################### end from pexacc2conf.pm #######################################

sub scorePPhrases( $$ ) {
	my( $infile, $outfhndl ) = @_;
	my( $outfbn );
	
	open( IN, "< " . $infile ) or die( "pdataworker::scorePPhrases: cannot open file " . $infile . " !\n" );
	binmode( IN, ":utf8" );

	$outfbn = <IN>;
	$outfbn =~ s/^\s+//;
	$outfbn =~ s/\s+$//;

	#For all phrase pairs 
	while ( my $line = <IN> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
		
		next if ( $line eq "" );
		next if ( $line =~ /^--param\s/ );
		
		my( $i, $j, $srcphr, $trgphr ) = split( /#SPLIT-HERE#/, $line );
		my( @srcphrtok ) = tokenizeText( $srcphr );
		my( @trgphrtok ) = tokenizeText( $trgphr );
		my( $pprob ) = scoreSentPair( \@srcphrtok, \@trgphrtok );
		
		print( $outfhndl $i . "#SPLIT-HERE#" . $j . "#SPLIT-HERE#" . $srcphr . "#SPLIT-HERE#" . $trgphr . "#SPLIT-HERE#" . $pprob . "\n" )
			if ( $pprob >= $OUTPUTTHR );
	}

	close( IN );	
}


sub readGIZAPPDict( $$$$ ) {
	my( $gizafile, $gizapp, $probthr, $resources ) = @_;
	my( $lcnt ) = 0;
	
	print( STDERR "pdataworker::readGIZAPPDict: reading file '$gizafile'...\n" )
		if ( $DEBUG );

	open( DICT, "< $gizafile" ) or die( "pdataworker::readGIZAPPDict : cannot open file \'$gizafile\' !\n" );
	binmode( DICT, ":utf8" );

	while ( my $line = <DICT> ) {
		$lcnt++;
		
		print( STDERR "pdataworker::readGIZAPPDict: read $lcnt lines...\n" )
			if ( $DEBUG && $lcnt % 100000 == 0 );
		
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;

		next if ( $line =~ /^$/ );
		
		my( $sprx ) = '\s+';
		
		$sprx = '\t+' if ( $line =~ /\t/ && $line =~ / / );

		my( @toks ) = split( /$sprx/, $line );
		#en - SRC, ro - TRG
		my( $enw ) = lc( $toks[0] );
		my( $row ) = lc( $toks[1] );
		my( $score ) = $toks[2];
		
		next if ( $score < $probthr );
		
		my( $subreplshtz ) = sub {
			my( $word ) = @_;
			my( %variants ) = ();
			
			#Sh and tz variations for Romanian
			do {
				my( $templ ) = $word;
			
				$templ =~ s/\x{0163}/\x{021B}/g;
				$variants{$templ} = 1;
				$templ =~ s/\x{015F}/\x{0219}/g;
				$variants{$templ} = 1;
			};
		
			do {
				my( $templ ) = $word;
			
				$templ =~ s/\x{0163}/\x{021B}/g;
				$variants{$templ} = 1;
				$templ =~ s/\x{0219}/\x{015F}/g;
				$variants{$templ} = 1;
			};

			do {
				my( $templ ) = $word;
			
				$templ =~ s/\x{021B}/\x{0163}/g;
				$variants{$templ} = 1;
				$templ =~ s/\x{015F}/\x{0219}/g;
				$variants{$templ} = 1;
			};
		
			do {
				my( $templ ) = $word;
			
				$templ =~ s/\x{021B}/\x{0163}/g;
				$variants{$templ} = 1;
				$templ =~ s/\x{0219}/\x{015F}/g;
				$variants{$templ} = 1;
			};
			
			$variants{$word} = 1;
			
			return keys( %variants );
		};
		
		foreach my $e ( $subreplshtz->( $enw ) ) {
			foreach my $r ( $subreplshtz->( $row ) ) {
				my( $le ) = lemmatizeWord( $e, $resources->{"enInflections"} );
				my( $lr ) = lemmatizeWord( $r, $resources->{"roInflections"} );
				my( $tpair );

				if ( $LEMMAS ) {
					$tpair = $le . "#" . $lr;
				}
				else {
					$tpair = $e . "#" . $r;
				}

				#Due to lemmatization, we may have multiple entries... select the maximum score.
				if ( ! exists( $gizapp->{$tpair} ) ) {
					$gizapp->{$tpair} = $score;
				}
				elsif ( $gizapp->{$tpair} < $score ) {
					$gizapp->{$tpair} = $score;
				}			
			} #end all ro variants
		} #end all en variants

	} #end dict

	close( DICT );
	return $gizapp;
}

#Union combination.
sub combineDictionaries( $$ ) {
	my( $base, $learnt ) = @_;
	my( $combined ) = {};
	
	#Only if the pair is in both dictionaries, combine the score.
	#Else, keep the score from each dictionary.
	foreach my $wp ( keys( %{ $base } ) ) {
		if ( exists( $learnt->{$wp} ) ) {
			$combined->{$wp} = $base->{$wp} * $DICTWEIGHTMAIN + $learnt->{$wp} * $DICTWEIGHTLEARNT;
			delete( $learnt->{$wp} );
		}
		else {
			$combined->{$wp} = $base->{$wp};
		}
	}
	
	foreach my $wp ( keys( %{ $learnt } ) ) {
		$combined->{$wp} = $learnt->{$wp};
	}	
	
	return $combined;
}

# symmetrical ready
sub pairInDict( $$$ ) {
	my( $srcw, $trgw, $res ) = @_;
	my( $gizapp ) = $res->{"enroDict"};
	
	$srcw = lc( $srcw );
	$trgw = lc( $trgw );
	
	my( $lsrcw ) = lemmatizeWord( $srcw, $res->{"enInflections"} );
	my( $ltrgw ) = lemmatizeWord( $trgw, $res->{"roInflections"} );
	
	foreach my $d ( $gizapp ) {
		my( $tpair ) = $srcw . "#" . $trgw;
		
		$tpair = $lsrcw . "#" . $ltrgw if ( $LEMMAS );
		
		return $d->{$tpair} if ( exists( $d->{$tpair} ) );
		return $d->{lc( $tpair )} if ( exists( $d->{lc( $tpair )} ) );
	}
	
	return 0;
}

sub scoreSentPair( $$ ) {
	my( $srcsentin, $trgsentin ) = @_;
	my( $score1 ) = scoreSentPairLP( $srcsentin, $trgsentin, \%ENRORES );
	my( $score2 ) = scoreSentPairLP( $trgsentin, $srcsentin, \%ROENRES );
	
	#The minimum between the two.
	#return ( ( $score1 <= $score2 ) ? $score1 : $score2 );
	return ( $score1 + $score2 ) / 2;
}

#This is the object with all the resources.
#my( %ENRORES ) = (
#	"enLang" => $SRCL,
#	"roLang" => $TRGL,
#	"enInflections" => \%INFLEN,
#	"roInflections" => \%INFLRO,
#	"enStopWords" => \%ENSTOPWORDS,
#	"roStopWords" => \%ROSTOPWORDS,
#	"enroDict" => $ENRODICT
#);

#symmetrical measure ready
sub scoreSentPairLP( $$$ ) {
	my( $srcsentin, $trgsentin, $resources ) = @_;
	
	#Set the resources...
	my( $srclang ) = $resources->{"enLang"};
	my( $trglang ) = $resources->{"roLang"};
	my( $srcstopw ) = $resources->{"enStopWords"};
	my( $trgstopw ) = $resources->{"roStopWords"};
	
	my( @srcsentarr ) = @{ $srcsentin };
	my( @trgsentarr ) = @{ $trgsentin };
	
	return 0 if ( scalar( @srcsentarr ) == 0 || scalar( @trgsentarr ) == 0 );
	
	pop( @srcsentarr ) if ( $srcsentarr[$#srcsentarr] eq "#EOS#" );
	pop( @trgsentarr ) if ( $trgsentarr[$#trgsentarr] eq "#EOS#" );
	
	return 0 if ( scalar( @srcsentarr ) == 0 || scalar( @trgsentarr ) == 0 );
	
	return 0 if ( scalar( @srcsentarr ) / scalar( @trgsentarr ) > $SENTRATIO ||
					scalar( @trgsentarr ) / scalar( @srcsentarr ) > $SENTRATIO );
	
	#Filtering, ver 4.1
	my( $srcsentstr ) = join( "", @srcsentarr );
	my( $trgsentstr ) = join( "", @trgsentarr );
	
	return 0 if ( $srcsentstr !~ /\p{IsAlpha}/ || $trgsentstr !~ /\p{IsAlpha}/ );
	
	#my( $srcsent ) = \@srcsentarr;
	#my( $trgsent ) = \@trgsentarr;
	
	#Only content words in there...
	my( $srcsent ) = [];
	my( $trgsent ) = [];
	
	foreach my $w ( @srcsentarr ) {
		next if ( $w =~ /^[^[:alnum:]]+$/ );
		next if ( exists( $srcstopw->{lc( $w )} ) );
		
		push( @{ $srcsent }, $w );
	}
	
	foreach my $w ( @trgsentarr ) {
		next if ( $w =~ /^[^[:alnum:]]+$/ );
		next if ( exists( $trgstopw->{lc( $w )} ) );
		
		push( @{ $trgsent }, $w );
	}	
	
	my( $probsum ) = 0;
	my( @probs ) = ();
	my( $HALFWINCONTENT ) = 5;
	my( $foundcontentword ) = 0;

	my( %maxjskip ) = ();
	my( $srcsentlennopunct ) = 0;
	my( @foundteqlines ) = ();
	
	for(  my $i = 0; $i < scalar( @{ $srcsent } ); $i++ ) {
		my( $w1 ) = $srcsent->[$i];
		
		#next if ( $w1 =~ /^[^[:alnum:]]+$/ );
		#next if ( exists( $srcstopw->{lc( $w1 )} ) );
		
		$srcsentlennopunct++;
		
		my( $halfwin ) = $HALFWINCONTENT;
		my( $trglandingj ) = int( ( scalar( @{ $trgsent } ) / scalar( @{ $srcsent } ) ) * $i );
		my( $trgleftidx ) = $trglandingj - $halfwin;
		my( $trgrightidx ) = $trglandingj + $halfwin;

		#Small bug fixed. From C# version of this measure.
		my( $adjustright ) = 0;
		my( $adjustleft ) = 0;
		
		if ( $trgleftidx < 0 ) {
			$trgleftidx = 0;
			$adjustright = 1;
		}
		
		if ( $trgrightidx >= scalar( @{ $trgsent } ) ) {
			$trgrightidx = scalar( @{ $trgsent } ) - 1;
			$adjustleft = 1;
		}
		
		if ( $adjustright ) {
			while ( $trgrightidx - $trgleftidx < 2 * $halfwin ) {
				$trgrightidx++;
			}

			if ( $trgrightidx >= scalar( @{ $trgsent } ) ) {
				$trgrightidx = scalar( @{ $trgsent } ) - 1;
			}		
		}
		
		if ( $adjustleft ) {
			while ( $trgrightidx - $trgleftidx < 2 * $halfwin ) {
				$trgleftidx--;
			}

			if ( $trgleftidx < 0 ) {
				$trgleftidx = 0;
			}
		}
		#end of small bug

		my( $maxp ) = 0;
		my( $maxj ) = -1;
		my( $foundteq ) = 0;
		
		for ( my $j = $trgleftidx; $j <= $trgrightidx; $j++ ) {
			next if ( exists( $maxjskip{$j} ) );
			
			my( $w2 ) = $trgsent->[$j];
			
			#next if ( $w2 =~ /^[^[:alnum:]]+$/ );
			#next if ( exists( $trgstopw->{lc( $w2 )} ) );
			
			my( $tpprob ) = 0;
			my( $w1w2ss ) = strsim::similarity( lc( normalizeWord( $w1, $srclang ) ), lc( normalizeWord( $w2, $trglang ) ) );
			
			if ( $w1 eq $w2 ) {
				$tpprob = 1;
				$foundteq = 1;
				push( @foundteqlines, "pdataworker::scoreSentPair[$srclang-$trglang]: found <'$w1', '$w2'> with prob = $tpprob.\n" );
			}
			elsif ( $w1w2ss >= $SSTHR ) {
				$tpprob = $w1w2ss;
				$foundteq = 1;
				push( @foundteqlines, "pdataworker::scoreSentPair[$srclang-$trglang]: found <'$w1', '$w2'> with prob = $tpprob.\n" );
			}
			else {
				my( $w1w2prob ) = pairInDict( $w1, $w2, $resources );
				
				if ( $w1w2prob > 0 ) {
					$tpprob = $w1w2prob;
					$foundteq = 1;
					push( @foundteqlines, "pdataworker::scoreSentPair[$srclang-$trglang]: found <'$w1', '$w2'> with prob = $tpprob.\n" );
				}
			}
			
			if ( $maxp < $tpprob ) {
				$maxp = $tpprob;
				$maxj = $j;
			}
		} #end trg
		
		if ( $foundteq ) {
			$probsum += $maxp;
			push( @probs, $maxp );
			$foundcontentword = 1;
			$maxjskip{$maxj} = [ $i, $maxp ] if ( $maxj >= 0 );
		}
	} #end src
	
	my( $score ) = 0;
	my( @heurprintlines ) = ();
	
	if ( scalar( @probs ) > 0 && $foundcontentword ) {
		#How many TEQs did we find
		my( $T ) = scalar( @probs );
		#How long is the source sentence (no punctuation)
		my( $S ) = $srcsentlennopunct;
		my( $averagesum ) = 0;
		
		foreach my $p ( @probs ) { 
			$averagesum += $p;
		}
		
		#Arithmetic mean destabilized by how many words from the source sentence have translations...
		my( $score1 ) = ( ( $T / $S ) ** ( $S / $T ) ) * ( $averagesum / $T );
		my( $score2 ) = 0;
		my( $featTranslationStrength ) = sub {
			return $score1;
		};
		my( @heuristics ) = (
			\&featStartOrEndWithTranslations,
			\&featHaveFinalPunct,
			\&featAlignNotScrambled,
			$featTranslationStrength
		);
		my( @rejectheuristics ) = (
			[ \&boolHaveSameNumbers, "boolHaveSameNumbers" ]
		);
		my( @heuristicsnamesandweights ) = (
			[ "featStartOrEndWithTranslations", 0.14 ],
			[ "featHaveFinalPunct", 0.02 ],
			[ "featAlignNotScrambled", 0.14 ],
			[ "featTranslationStrength", 0.7 ]
		);
		
		foreach my $hn ( @rejectheuristics ) {
			my( $heur, $hname ) = @{ $hn };
			
			if ( ! $heur->( $srcsent, $trgsent ) ) {
				if ( $DEBUG ) {
					print( STDERR "pdataworker::scoreSentPair[$srclang-$trglang]:-----------------------------------------------------------\n" );
					print( STDERR join( "", @foundteqlines ) );
					print( STDERR "pdataworker::scoreSentPair[$srclang-$trglang]:\n" );
					print( STDERR "\t" . join( " ", @{ $srcsent } ) . "\n" );
					print( STDERR "\t" . join( " ", @{ $trgsent } ) . "\n" );
					print( STDERR "\t" . "REJECTED by '$hname'" . "\n" );
					print( STDERR "pdataworker::scoreSentPair[$srclang-$trglang]:-----------------------------------------------------------\n" );
				}
				
				return 0;
			}
		}
		
		for ( my $k = 0; $k < scalar( @heuristics ); $k++ ) {
			my( $h ) = $heuristics[$k];
			my( $hn ) = $heuristicsnamesandweights[$k]->[0];
			my( $hw ) = $heuristicsnamesandweights[$k]->[1];
			my( $hval ) = 0;
			
			if ( $hn eq "featAlignNotScrambled" || $hn eq "featStartOrEndWithTranslations" ) {
				$hval = $h->( \%maxjskip, scalar( @{ $srcsent } ) - 1, scalar( @{ $trgsent } ) - 1 );
			}
			else {
				$hval = $h->( $srcsent, $trgsent );
			}
			
			$score2 += $hval * $hw;
			push( @heurprintlines, "pdataworker::scoreSentPair[$srclang-$trglang]: '$hn' is '" . $hval * $hw . "'.\n" );
		}
		
		$score = $score2;
	}
	
	if ( $score > 0 ) {
		if ( $DEBUG ) {
			print( STDERR "pdataworker::scoreSentPair[$srclang-$trglang]:-----------------------------------------------------------\n" );
			print( STDERR join( "", @foundteqlines ) );
			print( STDERR join( "", @heurprintlines ) );
			print( STDERR "pdataworker::scoreSentPair[$srclang-$trglang]:\n" );
			print( STDERR "\t" . join( " ", @{ $srcsent } ) . "\n" );
			print( STDERR "\t" . join( " ", @{ $trgsent } ) . "\n" );
			print( STDERR "\t" . $score . "\n" );
			print( STDERR "pdataworker::scoreSentPair[$srclang-$trglang]:-----------------------------------------------------------\n" );
		}
	}
	
	return $score;
}

sub isNamedEntity( $ ) {
	return 1 if ( $_[0] =~ /^[0-9:,.]+$/ );
	return 1 if ( $_[0] =~ /^\p{IsUpper}\p{IsLower}+$/ );
	return 1 if ( $_[0] =~ /^\p{IsUpper}+$/ );
	return 0;
}

#0 or 1
sub boolHaveSameNumbers( $$ ) {
	my( $srcsent, $trgsent ) = @_;
	my( %foundsrcent ) = ();
	
	for ( my $i = 0; $i < scalar( @{ $srcsent } ); $i++ ) {
		my( $srcw ) = $srcsent->[$i];
		
		if ( $srcw =~ /^[0-9][0-9\/:;.,-]*[0-9]$/ || $srcw =~ /^[0-9]$/ ) {
			$foundsrcent{$srcw} = 1;
		}
	}

	for ( my $i = 0; $i < scalar( @{ $trgsent } ); $i++ ) {
		my( $trgw ) = $trgsent->[$i];
		
		if ( $trgw =~ /^[0-9][0-9\/:;.,-]*[0-9]$/ || $trgw =~ /^[0-9]$/ ) {
			if ( ! exists( $foundsrcent{$trgw} ) ) {
				return 0;
			}
			else {
				delete( $foundsrcent{$trgw} );
			}
		}
	}		
	
	return 0 if ( scalar( keys( %foundsrcent ) ) > 0 );
	return 1;
}

#0 or 1
sub featHaveFinalPunct( $$ ) {
	my( @srcsent ) = @{ $_[0] };
	my( @trgsent ) = @{ $_[1] };
	my( $lastsrcw ) = pop( @srcsent );
	my( $lasttrgw ) = pop( @trgsent );
	
	return 0 if ( $lastsrcw =~ /^[^[:alnum:]]+$/ && $lasttrgw !~ /^[^[:alnum:]]+$/ );
	return 0 if ( $lastsrcw !~ /^[^[:alnum:]]+$/ && $lasttrgw =~ /^[^[:alnum:]]+$/ );
	
	return 1;
}

#0 or 1
#Structure:
#{ target js to source is along with teq prob }.
# j => [ i, 0.3 ]
sub featStartOrEndWithTranslations( $$$ ) {
	my( $maxjskip, $maxi, $maxj ) = @_;
	my( @sortj ) = sort { $a <=> $b } keys( %{ $maxjskip } );
	my( $lefttranslate ) = 0;
	my( $righttranslate ) = 0;
	my( $j ) = 0;
	
	while ( $j <= $#sortj && $sortj[$j] <= 2 ) {
		if ( $maxjskip->{$sortj[$j]}->[0] <= 2 ) {
			$lefttranslate = 1 if ( $maxjskip->{$sortj[$j]}->[1] >= $SUREGIZAPPTHR );
			last;
		}
		
		$j++;
	}

	$j = $#sortj;

	while ( $j >= 0 && abs( $maxj - $sortj[$j] ) <= 2 ) {
		if ( abs( $maxi - $maxjskip->{$sortj[$j]}->[0] ) <= 2 ) {
			$righttranslate = 1 if ( $maxjskip->{$sortj[$j]}->[1] >= $SUREGIZAPPTHR );
			last;
		}
		
		$j--;
	}	
		
	return $lefttranslate & $righttranslate;
}

#0..1 (close to 1 not scrambled)
sub featAlignNotScrambled( $$$ ) {
	my( $maxjskip, $maxi, $maxj ) = @_;
	
	return 0 if ( scalar( keys( %{ $maxjskip } ) ) == 0 );
	return 0 if ( $maxi == 0 || $maxj == 0 );
	
	my( $scrambled ) = 0;
	
	foreach my $j ( keys( %{ $maxjskip } ) ) {
		$scrambled += abs( $j / $maxj - $maxjskip->{$j}->[0] / $maxi );
	}

	my( $alno ) = scalar( keys( %{ $maxjskip } ) );
	
	return 1 - $scrambled / $alno;
}

#3.0 ok.
sub readStopWordsList( $ ) {
	my( %swl ) = ();

	open( SWL, "< $_[0]" ) or die( "pdataworker::readStopWordsList: cannot open file '$_[0]' !\n" );
	binmode( SWL, ":utf8" );

	while ( my $line = <SWL> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;

		#Sh and tz variations for Romanian
		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{0163}/\x{021B}/g;
			$swl{lc( $templ )} = 1;
			$templ =~ s/\x{015F}/\x{0219}/g;
			$swl{lc( $templ )} = 1;
		};
		
		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{0163}/\x{021B}/g;
			$swl{lc( $templ )} = 1;
			$templ =~ s/\x{0219}/\x{015F}/g;
			$swl{lc( $templ )} = 1;
		};

		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{021B}/\x{0163}/g;
			$swl{lc( $templ )} = 1;
			$templ =~ s/\x{015F}/\x{0219}/g;
			$swl{lc( $templ )} = 1;
		};
		
		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{021B}/\x{0163}/g;
			$swl{lc( $templ )} = 1;
			$templ =~ s/\x{0219}/\x{015F}/g;
			$swl{lc( $templ )} = 1;
		};

		$swl{lc( $line )} = 1;
	} #end all stop words

	close( SWL );
	return %swl;
}

#3.0 ok.
sub normalizeWord( $$ ) {
	my( $word ) = $_[0];
	my( $lang ) = $_[1];

	if ( ( $SRCL eq "en" && $TRGL eq "ro" ) || ( $SRCL eq "ro" && $TRGL eq "en" ) ) {
		#en-ro specific normalizations...
		#acirc
		$word =~ s/\x{00E2}/a/g if ( $lang eq "ro" );
		#Acirc
		$word =~ s/\x{00C2}/A/g if ( $lang eq "ro" );
		#icirc
		$word =~ s/\x{00EE}/i/g if ( $lang eq "ro" );
		#Icirc
		$word =~ s/\x{00CE}/I/g if ( $lang eq "ro" );
		#abreve
		$word =~ s/\x{0103}/a/g if ( $lang eq "ro" );
		#Abreve
		$word =~ s/\x{0102}/A/g if ( $lang eq "ro" );
		#scedil
		$word =~ s/\x{015F}/s/g if ( $lang eq "ro" );
		$word =~ s/\x{0219}/s/g if ( $lang eq "ro" );
		#Scedil
		$word =~ s/\x{015E}/S/g if ( $lang eq "ro" );
		$word =~ s/\x{0218}/S/g if ( $lang eq "ro" );
		#tcedil
		$word =~ s/\x{0163}/t/g if ( $lang eq "ro" );
		$word =~ s/\x{021B}/t/g if ( $lang eq "ro" );
		#Tcedil
		$word =~ s/\x{0162}/T/g if ( $lang eq "ro" );
		$word =~ s/\x{021A}/T/g if ( $lang eq "ro" );
		
		#'f' for 'ph'
		$word =~ s/[pP][hH]/f/g if ( $lang eq "en" );
		#remove 'h' in front of a consonant
		$word =~ s/[Hh][^aeiouy]//g if ( $lang eq "en" );
		#remove double consonant
		$word =~ s/([^aeiouy])\1/$1/g if ( $lang eq "en" );
		#other things go here...
	}

	return $word;
}

#3.0 ok.
sub lemmatizeWord( $$ ) {
	my( $word, $infllist ) = @_;
	
	#Endings are in lowercase
	$word = lc( $word );

	my( @wordlett ) = split( //, $word );
	my( $lemword ) = $word;
	
	#Match the longest suffix ...
	for ( my $i = $#wordlett; $i >= 1; $i-- ) {
		my( $crtsfx ) = join( "", @wordlett[$i .. $#wordlett] );

		if ( exists( $infllist->{$crtsfx} ) ) {
			$lemword = $word;
			$lemword =~ s/${crtsfx}$//;
		}	
	}

	return $lemword;
}

#3.0 ok.
sub readInflectionList( $ ) {
	my( %infl ) = ( "LONGEST" => 0 );
	
	open( INF, "< $_[0]" ) or die( "pdataworker::readInflectionList: cannot open file '$_[0]' !\n" );
	binmode( INF, ":utf8" );

	while ( my $line = <INF> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;

		#Sh and tz variations for Romanian
		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{0163}/\x{021B}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
			$templ =~ s/\x{015F}/\x{0219}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
		};
		
		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{0163}/\x{021B}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
			$templ =~ s/\x{0219}/\x{015F}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
		};

		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{021B}/\x{0163}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
			$templ =~ s/\x{015F}/\x{0219}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
		};
		
		do {
			my( $templ ) = $line;
			
			$templ =~ s/\x{021B}/\x{0163}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
			$templ =~ s/\x{0219}/\x{015F}/g;
			$infl{lc( $templ )} = length( lc( $templ ) );
		};
		
		$infl{lc( $line )} = length( lc( $line ) );
		
		if ( $infl{"LONGEST"} < length( lc( $line ) ) ) {
			$infl{"LONGEST"} = length( lc( $line ) );
		}
	}
	
	close( INF );
	return %infl;
}

#3.0 ok.
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

#3.0 ok.
sub readInputParams( $ ) {
	my( $outfbn );
	my( @docsp ) = ();
	my( %conf ) = ();

	open( IN, "< " . $_[0] ) or die( "pdataworker::readInputParams: cannot open file " . $_[0] . " !\n" );
	binmode( IN, ":utf8" );

	$outfbn = <IN>;
	$outfbn =~ s/^\s+//;
	$outfbn =~ s/\s+$//;

	while ( my $line = <IN> ) {
		$line =~ s/^\s+//;
		$line =~ s/\s+$//;
		
		next if ( $line eq "" );
		
		if ( $line =~ /^--param\s/ ) {
			$line =~ s/^--param\s//;
			
			my( $param, $value ) = split( /\s*=\s*/, $line );
			
			$conf{$param} = $value;
			next;
		}
		else {
			last;
		}
	}

	close( IN );

	return ( $outfbn, \%conf );
}
