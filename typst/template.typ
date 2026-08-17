#let script-size = 7.97224pt
#let footnote-size = 8.50012pt
#let small-size = 9.24994pt
#let normal-size = 10.00002pt
#let large-size = 11.74988pt

#let rfc(
  title: [Paper title],
  authors: (),
  abstract: none,
  bibliography: none,
  body,
) = {
  set text(lang: "en", region: "GB", size: normal-size, font: "New Computer Modern")
  set document(title: title, author: authors)
  set page(
    numbering: "1.",
    paper: "a4",
    margin: (
      top: 117pt,
      left: 118pt,
      right: 119pt,
      bottom: 96pt,
    ),
    header: context {
      let i = counter(page).get().first()
      if i == 1 { return }
      set text(size: script-size)
      grid(
        columns: (6em, 1fr, 6em),
        align: (start, center, end),
        if calc.even(i) [#i],
        upper(
          if calc.odd(i) { title } else { authors.join(", ", last: ", and ") }
        ),
        if calc.odd(i) { [#i] }
      )
    },
    footer-descent: 12pt,
    footer: context {
      let i = counter(page).get().first()
      if i == 1 {
        align(center, text(size: script-size, [#i]))
      }
    }
  )
  
  set heading(numbering: "1.")
  show heading: it => {
    let number = if it.numbering != none {
      counter(heading).display(it.numbering)
      h(7pt, weak: true)
    }
    set text(size: normal-size, weight: 400)
    set par(first-line-indent: 0em)
    if it.level == 1 {
      colbreak(weak: true)
      set align(center)
      set text(size: normal-size)
      smallcaps[
        #v(15pt, weak: true)
        #number
        #it.body
        #v(normal-size, weak: true)
      ]
    } else {
      v(11pt, weak: true)
      number
      let styled = if it.level == 2 { strong } else { emph }
      styled(it.body + [. ])
      h(7pt, weak: true)
    }
  }  
  show terms: set block(spacing: 2em); show terms: block
  set list(indent: 24pt, body-indent: 5pt)
  set enum(indent: 24pt, body-indent: 5pt)
  show link: set text(font: "New Computer Modern Mono")
  show divider: set align(center)
  show divider: set block(spacing: 1.5em)
  
  set figure(gap: 17pt)
  show figure: set block(above: 12.5pt, below: 15pt)
  show figure: it => {
    show figure.caption: caption => {
      show: pad.with(x: 2em)
      smallcaps(caption.supplement)
      if caption.numbering != none {
        [ ]
        numbering(caption.numbering, ..caption.counter.at(it.location()))
      }
      [. ]
      caption.body
    }
    show selector.or(table, image): pad.with(x: 23pt)
    it
  }
  show figure.where(kind: "packet"): set figure(supplement: [Packet Diagram])

  
  show std.title: set text(size: large-size, weight: 700)
  v(2em, weak: true)
  align(center, upper({
    std.title()
    v(2em, weak: true)
    grid(
      columns: authors.len() * (1fr,),
      gutter: 12pt,
      ..authors.map(author => align(center, text(size: footnote-size, author)))
    )
  }))
  
  set par(spacing: 0.58em, first-line-indent: (amount: 1.2em, all: false), justify: true, leading: 0.58em)
  if abstract != none {
    v(2em, weak: true)
    set text(script-size)
    show: pad.with(x: 3.5em)
    smallcaps[Summary. ]
    abstract
  }
  block(above: 2em, outline(depth: 2, title: none))

  body
  
  show std.bibliography: set text(8pt)
  set std.bibliography(title: [References])
  if bibliography != none {
    pagebreak(weak: true)
    show std.bibliography: set block(above: 11pt)
    show std.bibliography: pad.with(x: 0.5pt)
    bibliography
  }
}

#let PacketLayout(name, fields: ()) = {
  let field(name: none, bits: none, value: none, type: none, indentation: 0) = {
    "\t" * indentation
    if type != none {
      if name != none { name + " = " }
      type
      " {\n" + value.map(x => field(indentation: indentation + 1, ..x) + ",\n").join(default: " ")
      "\t" * indentation
      "}"
    } else {
      name
      if bits != none { " (" + str(bits) + ")" }
      if value != none { " = " + str(value) }
    }
  }
  raw(
    field(type: name, value: fields),
    lang: "packet",
    block: true,
    // This is trimmed LLM output, didn't want to learn sublime text's YAML syntax parsing spec
    syntaxes: bytes(```yaml
      file_extensions: [ packet ]    
      scope: source.packet
      contexts:
        main:
          - match: '\b([A-Z][A-Za-z0-9 ]*)(?=\s*(?:\(|\{))'
            captures:
              1: entity.name.type.packet
          - match: '\('
            scope: punctuation.section.parens.begin.packet
            push: declaration-parameters
          - match: '[{}]'
            scope: punctuation.section.block.packet
          - match: '^\s*([A-Za-z][A-Za-z0-9 ]*)'
            captures:
              1: variable.other.member.packet
            push: after-field

        after-field:
          - match: '\('
            scope: punctuation.section.parens.begin.packet
            push: width
          - match: ':'
            scope: keyword.operator.type.packet
            push: type-reference
          - match: '='
            scope: keyword.operator.assignment.packet
            push: expression
          - match: ','
            scope: punctuation.separator.sequence.packet
            pop: true
          - match: '$'
            pop: true

        width:
          - meta_scope: meta.annotation.width.packet
          - match: '0[xX][0-9A-Fa-f]+'
            scope: constant.numeric.hex.packet
          - match: '\d+'
            scope: constant.numeric.integer.packet
          - match: '\.\.'
            scope: keyword.operator.range.packet
          - match: '\)'
            scope: punctuation.section.parens.end.packet
            pop: true
      
        declaration-parameters:
          - meta_scope: meta.parameters.packet
          - match: '\b[a-z][A-Za-z0-9_]*\b'
            scope: variable.parameter.packet
          - match: ','
            scope: punctuation.separator.sequence.packet
          - match: '\)'
            scope: punctuation.section.parens.end.packet
            pop: true
      
        type-reference:
          - match: '\b([A-Z][A-Za-z0-9 ]*)'
            captures:
              1: support.type.packet
          - match: '\('
            scope: punctuation.section.parens.begin.packet
            push: constructor-arguments
          - match: ','
            scope: punctuation.separator.sequence.packet
            pop: true
          - match: '$'
            pop: true
      
        constructor-arguments:
          - meta_scope: meta.type.arguments.packet
          - match: '0[xX][0-9A-Fa-f]+'
            scope: constant.numeric.hex.packet
          - match: '\d+'
            scope: constant.numeric.integer.packet
          - match: '\b[a-z][A-Za-z0-9_]*\b'
            scope: variable.parameter.packet
          - match: ','
            scope: punctuation.separator.sequence.packet
          - match: '\)'
            scope: punctuation.section.parens.end.packet
            pop: true
      
        expression:
          - match: '0[xX][0-9A-Fa-f]+'
            scope: constant.numeric.hex.packet
          - match: '\d+'
            scope: constant.numeric.integer.packet
          - match: '\b[a-z][A-Za-z0-9_]*\b'
            scope: variable.parameter.packet
          - match: ','
            scope: punctuation.separator.sequence.packet
            pop: true
          - match: '$'
            pop: true
    ```.text),
  )
}

#let PacketFigure(caption: [], ..args) = figure(
  kind: "packet",
  caption: [#args.pos().at(0)] + caption,
  PacketLayout(..args),
)

#import "@preview/cetz:0.4.0": canvas, tree.tree

#let MerkleTree() = {}
