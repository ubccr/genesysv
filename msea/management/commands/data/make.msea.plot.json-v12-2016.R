### Command-line input: 1. gene: gene name
###                     2. output path
###                     3. vset: which variant set of the given gene to plot
###                     TODO: 1. Restructure command-line code to deal with multiple types of input
###                           2. Implement argparse package for better command-line parsing

library(plyr)
library(ggplot2) # must be version 1.0.1 or older!
library(grid)
library(jsonlite)

generate_mas_plot <- function(gene_info,output_file,domain_list) {
    mas <- as.data.frame(gene_info$mutation_accumulation_score[c(FALSE,TRUE)])
    names(mas)[1] <- 'y'
    y_floor = min(mas$y)
    y_ceiling <- round_any(max(gene_info$mutation_accumulation_score[c(FALSE,TRUE)]), .01, f=ceiling)
    x_ceiling <- round_any(gene_info$length, 100, f=ceiling)
    y_range <- max(mas)-min(mas)
    seq_line_y <- min(mas)-(y_range*0.05)

    #convert list of domains to data frame
    if (!is.null(domain_list)) {
        drows <- length(domain_list$domain.start)
        domain_info <- data.frame(symbol = rep(domain_list$symbol, drows),
                                  refseq.ID = rep(domain_list$refseq.ID, drows),
                                  protein.ID = rep(domain_list$protein.ID, drows),
                                  start = as.integer(domain_list$domain.start),
                                  end = as.integer(domain_list$domain.end),
                                  name = domain_list$domain.name,
                                  type = domain_list$domain.type,
                                  stringsAsFactors=F)
        if (! is.null(domain_list$zscore)){
            domain_info$zscore <- (as.double(domain_list$zscore)-1.64)/(6.38-1.64)
            domain_info$zscore <- ifelse(domain_info$zscore<0.15,.15,domain_info$zscore)

        }
    }

    # ensure mut_loc is in increasing order and associated vectors match up with it
    mut_loc = gene_info$mutations$start[c(TRUE,FALSE)]
    freq = gene_info$mutations$frequencies[c(FALSE,TRUE)]
    exonic_func = gene_info$mutations$exonic_functions[c(FALSE,TRUE)]
    mutation_types = gene_info$mutations$mutation_types[c(FALSE,TRUE)]
    deleterious = gene_info$mutations$deleterious[c(FALSE,TRUE)]
    if (mut_loc[1] > rev(mut_loc)[1]) {
        mut_loc <- rev(mut_loc)
        deleterious <- rev(deleterious)
        mutation_types <- rev(mutation_types)
        exonic_func <- rev(exonic_func)
        freq <- rev(freq)
    }
    mut_loc <- as.data.frame(mut_loc)
    names(mut_loc)[1] <- 'x'
    mut_loc$deleterious <- factor(deleterious, level=c('No','Yes','Unknown'))
    mut_loc$mutation_types <- factor(mutation_types, level=c('SNP','INDEL'))
    mut_loc$exonic_func <- exonic_func
    mut_loc$freq <- freq
    mut_loc$y <- seq_line_y+(y_range*0.03)

    # unabbreviate exonic_func
    exonic_func[grepl('^frameshift|stopgain',exonic_func)] <- 'Loss of Function'
    exonic_func[grepl('stoploss',exonic_func)] <-  'Stoploss'
    exonic_func[grepl('nonframeshift|nonsynonymous',exonic_func)] <- 'Missense'
    exonic_func[grepl('synonymous_SNV',exonic_func)] <- 'Synonymous SNV'

    # unabbreviate vset
    vset_map <- c(ans   = 'All nonsilent SNPs',
                  ansi  = 'All nonsilent SNPs and Indels',
                  dns   = 'Deleterious nonsilent SNVs',
                  dnsi  = 'Deleterious nonsilent SNVs and Indels',
                  ass   = 'All silent SNVs',
                  asbns = 'All silent SNVs plus benign nonsilent SNVs',
                  prom  = 'Promoter',
                  lof   = 'Loss of Function',
                  miss  = 'Missense')
    vset <- vset_map[[gene_info$vset]]

    plot_title <- paste0(gene_info$gene_name, ' (', gene_info$refgene_id, '), ', vset,
                         ' -- P-Value: ', format(gene_info$pvalue, digits = 5),
                         ' -- NES: ', format(gene_info$nes, digits = 5))
    palette <- c('#CC79A7','#D55E00','#0072B2','#F0E442','#009E73','#56B4E9','#E69F00','#999999')
    arrow = arrow(angle=15,length=unit(.15,'inches'),end='both',type='closed')
    seq.line <- annotate('segment', x=1, xend=gene_info$length, y=seq_line_y, yend=seq_line_y, color='gray', size=3)

    # find proper MES line and create related components accordingly
    mes_max_backwards <- abs(max(mas$y) - min(mas$y[1:which.max(mas$y)]))
    mes_min_forwards <- abs(max(mas$y[which.min(mas$y):length(mas$y)]) - min(mas$y))

    if (mes_min_forwards > mes_max_backwards) { # largest MES starts at global minimum but does not end at global max
        bottom.dashed <- geom_segment(data=mas,linetype='dashed',size=.25,
                                      aes(x=which.min(y)*1.01,
                                          xend=length(y)*1.01,
                                          y=min(y),
                                          yend=min(y)))
        top.dashed <- geom_segment(data=mas,linetype='dashed',size=.25,
                                   aes(x=which.max(y[which.min(y):length(y)])+which.min(y)*1.01,
                                       xend=length(y)*1.01,
                                       y=max(y[which.min(y):length(y)]),
                                       yend=max(y[which.min(y):length(y)])))
        mes.line <- geom_segment(data=mas,arrow=arrow,
                                 aes(x=length(y)*1.01,
                                     xend=length(y)*1.01,
                                     y=min(y),
                                     yend=max(y[which.min(y):length(y)])))
        mes.text <- annotate('text',label='MES',angle=90,size=4,vjust=2,
                             x=gene_info$length*1.01,
                             y=(min(mas$y)+max(mas$y[which.min(mas$y):length(mas$y)]))/2)
    } else { # largest MES ends at global maximum
        bottom.dashed <- geom_segment(data=mas,linetype='dashed',size=.25,
                                      aes(x=which.min(y[1:which.max(y)])*1.01,
                                          xend=length(y)*1.01,
                                          y=min(y[1:which.max(y)]),
                                          yend=min(y[1:which.max(y)])))
        top.dashed <- geom_segment(data=mas,linetype='dashed',size=.25,
                                   aes(x=which.max(y)*1.01,
                                       xend=length(y)*1.01,
                                       y=max(y),
                                       yend=max(y)))
        mes.line <- geom_segment(data=mas,arrow=arrow,
                                 aes(x=length(y)*1.01,
                                     xend=length(y)*1.01,
                                     y=min(y[1:which.max(y)]),
                                     yend=max(y)))
        mes.text <- annotate('text',label='MES',angle=90,size=4,vjust=2,
                             x=gene_info$length*1.01,
                             y=(max(mas$y)+min(mas$y[1:which.max(mas$y)]))/2)
    }
    # browser()

    q <- ggplot()
    q <- q + seq.line
    q <- q + mes.line
    q <- q + mes.text
    q <- q + bottom.dashed
    q <- q + top.dashed
    q <- q + theme_bw()
    q <- q + theme(axis.line = element_line(),
                   panel.border = element_blank(),
                   plot.title=element_text(hjust = 0, size = 12),
                   axis.text.x = element_text(vjust = 0.5, angle = 90))
    if (vset=='Promoter') {
        q <- q + labs(title = plot_title, x='Nucleotide Sequence', y='Mutation Accumulation Score (MAS)')
        if (nrow(domain_info)>0) {
        #if (length(domain_info[[1]])>0) {
            domain_info$ymin <- seq_line_y-(y_range*0.015)
            domain_info$ymax <- seq_line_y+(y_range*0.015)
            q <- q + geom_rect(data=domain_info, fill='blue', aes(xmin=start,xmax=end,ymin=ymin,ymax=ymax,alpha=zscore))
            #q <- q + scale_alpha_continuous(name='Motif\nZ score', range=c(0.5,1.0), limits=c(1,5))
            q <- q + scale_alpha_continuous(name='Motif\nZ score', range=c(0,1), limits=c(0,1))
            q <- q + labs(title = plot_title, x='Nucleotide Sequence', y='Mutation Accumulation Score (MAS)')
        }
    } else {
        q <- q + labs(title = plot_title, x='Amino Acid Sequence', y='Mutation Accumulation Score (MAS)')
        if (nrow(domain_info)>0) {
        #if (length(domain_info[[1]])>0) {
            # browser()
            domain_info$ymin <- ifelse(domain_info$type=='multi-dom',seq_line_y-(y_range*0.02),seq_line_y+(y_range*0.0025))
            domain_info$ymax <- ifelse(domain_info$type=='multi-dom',seq_line_y-(y_range*0.0025),seq_line_y+(y_range*0.02))
            if (length(unique(domain_info$type))==1) {
                domain_info$ymin <- seq_line_y-(y_range*0.015)
                domain_info$ymax <- seq_line_y+(y_range*0.015)
            }
            q <- q + geom_rect(data=domain_info, aes(xmin=start,xmax=end,ymin=ymin,ymax=ymax,fill=name))
            q <- q + scale_fill_discrete(name='Domain\nName')
        }
    }
    q <- q + scale_y_continuous(limits=c(y_floor-(y_range*0.1),y_ceiling+(y_range*0)),
                                breaks=seq(round_any(y_floor,.1,f=floor),y_ceiling,.05))
    if (x_ceiling <= 2000) {
        q <- q + scale_x_continuous(limits=c(1,x_ceiling+50),breaks=seq(0,x_ceiling,100))
    } else if (x_ceiling <= 5000) {
        q <- q + scale_x_continuous(limits=c(1,round_any(x_ceiling+50, 200, f=ceiling)),
                                    breaks=seq(0,round_any(x_ceiling, 200, f=ceiling),200))
    } else if (x_ceiling <= 10000) {
        q <- q + scale_x_continuous(limits=c(1,round_any(x_ceiling+50, 500, f=ceiling)),
                                    breaks=seq(0,round_any(x_ceiling, 500, f=ceiling),500))
    } else if (x_ceiling <= 20000) {
        q <- q + scale_x_continuous(limits=c(1,round_any(x_ceiling+50, 1000, f=ceiling)),
                                    breaks=seq(0,round_any(x_ceiling, 1000, f=ceiling),1000))
    } else {
        q <- q + scale_x_continuous(limits=c(1,round_any(x_ceiling+50, 2000, f=ceiling)),
                                    breaks=seq(0,round_any(x_ceiling, 2000, f=ceiling),2000))
    }
    q <- q + geom_line(data=mas, aes(x=seq_along(y), y=y), color='blue', stat='identity')
    q <- q + geom_point(data=mut_loc, aes(x=x,y=y,color=mutation_types), shape='|', size=2)
    q <- q + scale_color_manual(values=c('#1b9e77','#d95f02'),
                                name='Variant\nType',
                                limits=levels(mut_loc$mutation_types),
                                drop=T)
    q <- q + guides(color = guide_legend(order=1),
                    fill = guide_legend(order=2))

    svg(output_file,width=9,height=9)
    print(q)
    dev.off()
}

args <- commandArgs(trailingOnly = T)
gene_str <- args[1]
output_path <- args[2]
vset <- args[3]
domain_str <- args[4]

# gene_str <- paste0(output_path, '/', gene, '_', vset)
# domain_str <- paste0(output_path, '/', domain, '_', vset)
# load gene and domain info
#gene_str <- './NM_138420_ansi_gene.json'
gene_info <- fromJSON(gene_str)
#if (vset == 'prom') {
#    domain <- read.table('../input/tfbs.plot_ready.txt', header=T, stringsAsFactors=F, sep='\t')
#    domain_info <- domain[domain$refseq.ID==gene_info$gene_name, ]
#} else {
#    domain <- read.delim('../input/protein.gbk.regions.symbol.txt', header=T, stringsAsFactors=F, sep='\t')
#    domain_info <- domain[domain$refseq.ID==gene_info$refgene_id, ]
#}
#domain_str <- './NM_138420_ansi_domain.json'
domain_info <- fromJSON(domain_str)

domain_info$domain.name <- strtrim(domain_info$domain.name, 15)

output_file <- paste0(output_path, gene_info$gene_name, '-', gene_info$refgene_id, '-', vset, '.svg')
generate_mas_plot(gene_info,output_file,domain_info)
